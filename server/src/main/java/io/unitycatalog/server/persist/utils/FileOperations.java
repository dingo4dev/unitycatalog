package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.AwsUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class FileOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileOperations.class);
  private final ServerProperties serverProperties;
  private static String modelStorageRootCached;
  private static String modelStorageRootPropertyCached;

  public FileOperations(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  /**
   * TODO: Deprecate this method once unit tests are self contained and this class gets
   * re-instantiated with each test. Property updates shouldn't affect the instantiated class and we
   * should require a server restart if the properties file is updated.
   */
  private static void reset() {
    modelStorageRootPropertyCached = null;
    modelStorageRootCached = null;
  }

  // Model specific storage root handlers and convenience methods
  private String getModelStorageRoot() {
    String currentModelStorageRoot = serverProperties.get(Property.MODEL_STORAGE_ROOT);
    if (modelStorageRootPropertyCached != currentModelStorageRoot) {
      // This means the property has been updated from the previous read, or this is the first time
      // reading it
      reset();
    }
    if (modelStorageRootCached != null) {
      return modelStorageRootCached;
    }
    String modelStorageRoot = currentModelStorageRoot;
    if (modelStorageRoot == null) {
      // If the model storage root is empty, use the CWD
      modelStorageRoot = System.getProperty("user.dir");
    }
    // If the model storage root is not a valid URI, make it one
    if (!UriUtils.isValidURI(modelStorageRoot)) {
      // Convert to an absolute path
      modelStorageRoot = Paths.get(modelStorageRoot).toUri().toString();
    }
    // Check if the modelStorageRoot ends with a slash and remove it if it does
    while (modelStorageRoot.endsWith("/")) {
      modelStorageRoot = modelStorageRoot.substring(0, modelStorageRoot.length() - 1);
    }
    modelStorageRootCached = modelStorageRoot;
    modelStorageRootPropertyCached = currentModelStorageRoot;
    return modelStorageRoot;
  }

  private String getModelDirectoryURI(String entityFullName) {
    return getModelStorageRoot() + "/" + entityFullName.replace(".", "/");
  }

  public String getModelStorageLocation(String catalogId, String schemaId, String modelId) {
    return getModelDirectoryURI(catalogId + "." + schemaId + ".models." + modelId);
  }

  public String getModelVersionStorageLocation(
      String catalogId, String schemaId, String modelId, String versionId) {
    return getModelDirectoryURI(
        catalogId + "." + schemaId + ".models." + modelId + ".versions." + versionId);
  }

  private static URI createURI(String uri) {
    if (uri.startsWith("s3://") || uri.startsWith("file:")) {
      return URI.create(uri);
    } else {
      return Paths.get(uri).toUri();
    }
  }

  public void deleteDirectory(String path) {
    URI directoryUri = createURI(path);
    validateURI(directoryUri);
    if (directoryUri.getScheme() == null || directoryUri.getScheme().equals("file")) {
      try {
        deleteLocalDirectory(Paths.get(directoryUri));
      } catch (RuntimeException | IOException e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to delete directory: " + path, e);
      }
    } else if (directoryUri.getScheme().equals("s3")) {
      modifyS3Directory(directoryUri, false);
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + directoryUri.getScheme());
    }
  }

  private static void deleteLocalDirectory(Path dirPath) throws IOException {
    if (Files.exists(dirPath)) {
      try (Stream<Path> walk = Files.walk(dirPath, FileVisitOption.FOLLOW_LINKS)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + path, e);
                  }
                });
      }
    } else {
      throw new IOException("Directory does not exist: " + dirPath);
    }
  }

  private URI modifyS3Directory(URI parsedUri, boolean createOrDelete) {
    String bucketName = parsedUri.getHost();
    String path = parsedUri.getPath().substring(1); // Remove leading '/'
    String accessKey = serverProperties.getProperty("aws.s3.accessKey");
    String secretKey = serverProperties.getProperty("aws.s3.secretKey");
    String sessionToken = serverProperties.getProperty("aws.s3.sessionToken");
    String region = serverProperties.getProperty("aws.region");
    String endpointUrl = serverProperties.getProperty("aws.endpointUrl");

    AwsCredentialsProvider awsCredentialsProvider =
        AwsUtils.getAwsCredentialsProvider(accessKey, secretKey, sessionToken);
    S3Client s3Client = AwsUtils.getS3Client(awsCredentialsProvider, region, endpointUrl);

    if (createOrDelete) {

      if (!path.endsWith("/")) {
        path += "/";
      }
      if (AwsUtils.doesObjectExist(s3Client, bucketName, path)) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + path);
      }
      try {
        // Create a zero-byte object to represent the directory
        s3Client.putObject(
            PutObjectRequest.builder().bucket(bucketName).key(path).build(), RequestBody.empty());
        LOGGER.debug("Directory created successfully: {}", path);
        return URI.create(String.format("s3://%s/%s", bucketName, path));
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + path, e);
      }
    } else {
      ListObjectsV2Request req =
          ListObjectsV2Request.builder().bucket(bucketName).prefix(path).build();
      s3Client.listObjectsV2Paginator(req).stream()
          .flatMap(r -> r.contents().stream())
          .forEach(
              object -> {
                DeleteObjectRequest deleteRequest =
                    DeleteObjectRequest.builder().bucket(bucketName).key(object.key()).build();
                s3Client.deleteObject(deleteRequest);
              });

      return URI.create(String.format("s3://%s/%s", bucketName, path));
    }
  }

  private static URI adjustFileUri(URI fileUri) {
    String uriString = fileUri.toString();
    // Ensure the URI starts with "file:///" for absolute paths
    if (uriString.startsWith("file:/") && !uriString.startsWith("file:///")) {
      uriString = "file://" + uriString.substring(5);
    }
    return URI.create(uriString);
  }

  public static String convertRelativePathToURI(String url) {
    if (url == null) {
      return null;
    }
    if (isSupportedCloudStorageUri(url)) {
      return url;
    } else {
      return adjustFileUri(createURI(url)).toString();
    }
  }

  public static boolean isSupportedCloudStorageUri(String url) {
    String scheme = URI.create(url).getScheme();
    return scheme != null && Constants.SUPPORTED_SCHEMES.contains(scheme);
  }

  private static void validateURI(URI uri) {
    if (uri.getScheme() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid path: " + uri.getPath());
    }
    URI normalized = uri.normalize();
    if (!normalized.getPath().startsWith(uri.getPath())) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Normalization failed: " + uri.getPath());
    }
  }

  public static void assertValidLocation(String location) {
    validateURI(URI.create(location));
  }
}
