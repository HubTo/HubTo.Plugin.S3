using Amazon.S3;
using Amazon.S3.Model;
using HubTo.Abstraction.Enums;
using HubTo.Abstraction.Logging;
using HubTo.Abstraction.Models;
using HubTo.Abstraction.Registrars;

namespace HubTo.Plugin.S3;

public sealed class S3StoragePlugin : IStoragePlugin
{
    public string Name => "S3-Storage-Plugin";
    public PluginType Type => PluginType.Storage;

    private IAmazonS3? _client;
    private string? _bucketName;
    private IHubToLogger? _logger;

    public Task InitializeAsync(IDictionary<string, string> settings, IHubToLogger logger, CancellationToken cancellationToken = default)
    {
        _logger = logger;
        _logger.LogInformation("Initializing S3 Storage Plugin...");

        settings.TryGetValue("ServiceUrl", out var serviceUrl);
        settings.TryGetValue("AccessKey", out var accessKey);
        settings.TryGetValue("SecretKey", out var secretKey);
        settings.TryGetValue("Region", out var region);
        settings.TryGetValue("BucketName", out _bucketName);

        if (string.IsNullOrWhiteSpace(_bucketName))
            _logger.LogWarning("BucketName is not configured. Storage operations will fail.");

        var config = new AmazonS3Config
        {
            ServiceURL = serviceUrl ?? "http://localhost:9000",
            ForcePathStyle = true,
            AuthenticationRegion = region ?? "us-east-1"
        };

        try
        {
            _client = new AmazonS3Client(accessKey, secretKey, config);
            _logger.LogInformation($"S3 Client initialized successfully. Bucket: {_bucketName}");
        }
        catch (Exception ex)
        {
            _logger.LogError("Failed to initialize S3 Client.", ex);
        }

        return Task.CompletedTask;
    }

    public async Task<string> SaveAsync(Stream stream, string fileName, StorageMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            _logger?.LogWarning("SaveAsync called but S3 client is not initialized.");
            return string.Empty;
        }

        try
        {
            var safeKey = $"{Guid.NewGuid()}/{Path.GetFileName(fileName)}";

            _logger?.LogInformation($"Uploading file to S3. Key: {safeKey}");

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = safeKey,
                InputStream = stream,
                AutoCloseStream = true
            };

            if (metadata != null)
            {
                if (!string.IsNullOrEmpty(metadata.ContentType))
                    request.ContentType = metadata.ContentType;

                if (metadata.Tags != null)
                {
                    foreach (var tag in metadata.Tags)
                        request.TagSet.Add(new Tag { Key = tag.Key, Value = tag.Value });
                }
            }

            await _client.PutObjectAsync(request, cancellationToken);
            return safeKey;
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Failed to upload file: {fileName}", ex);
            return string.Empty;
        }
    }

    public async Task<Stream> GetAsync(string fileId, CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            _logger?.LogWarning("GetAsync called but S3 client is not initialized.");
            return Stream.Null;
        }

        try
        {
            _logger?.LogInformation($"Fetching file from S3: {fileId}");

            using (var s3Response = await _client.GetObjectAsync(_bucketName, fileId, cancellationToken))
            {
                var ms = new MemoryStream();
                await s3Response.ResponseStream.CopyToAsync(ms, cancellationToken);
                ms.Position = 0;
                return ms;
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Failed to fetch file: {fileId}", ex);
            return Stream.Null;
        }
    }

    public async Task DeleteAsync(string fileId, CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            _logger?.LogWarning("DeleteAsync called but S3 client is not initialized.");
            return;
        }

        try
        {
            _logger?.LogInformation($"Deleting file from S3: {fileId}");
            await _client.DeleteObjectAsync(_bucketName, fileId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Failed to delete file: {fileId}", ex);
        }
    }

    public Task<string?> GetDownloadUrlAsync(string fileId, TimeSpan expires, CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            _logger?.LogWarning("GetDownloadUrlAsync called but S3 client is not initialized.");
            return Task.FromResult<string?>(null);
        }

        try
        {
            var request = new GetPreSignedUrlRequest
            {
                BucketName = _bucketName,
                Key = fileId,
                Expires = DateTime.UtcNow.Add(expires)
            };

            return Task.FromResult<string?>(_client.GetPreSignedURL(request));
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Failed to generate presigned URL for: {fileId}", ex);
            return Task.FromResult<string?>(null);
        }
    }

    public async Task<HealthStatus> CheckHealthAsync(CancellationToken ct = default)
    {
        if (_client is null)
            return new HealthStatus { IsHealthy = false, Details = "S3 client is not initialized." };

        try
        {
            await _client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = _bucketName,
                MaxKeys = 1
            }, ct);

            return new HealthStatus { IsHealthy = true, Details = "S3 connection is healthy." };
        }
        catch (Exception ex)
        {
            _logger?.LogError($"S3 health check failed for bucket: {_bucketName}", ex);
            return new HealthStatus { IsHealthy = false, Details = $"S3 connection failed: {ex.Message}" };
        }
    }

    public Task ShutdownAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Shutting down S3 Storage Plugin...");
        _client?.Dispose();
        return Task.CompletedTask;
    }
}