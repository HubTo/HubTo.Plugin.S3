using Amazon.S3;
using Amazon.S3.Model;
using HubTo.Abstraction.Enums;
using HubTo.Abstraction.Logging;
using HubTo.Abstraction.Models.Common;
using HubTo.Abstraction.Models.PluginResult;
using HubTo.Abstraction.Models.Storage;
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

    private PluginResult Guard()
    {
        if (string.IsNullOrWhiteSpace(_bucketName))
            return PluginResult.Fail("BucketName is not configured");

        if (_client is null)
            return PluginResult.Fail("S3 client is not initialized");

        return PluginResult.Ok();
    }

    public async Task<PluginResult<string>> SaveAsync(Stream stream, string fileName, StorageMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        var guard = Guard();
        if (!guard.IsSuccess)
            return PluginResult<string>.Fail(guard.Errors);

        try
        {
            string safeKey = fileName.Contains('/') ? fileName : $"{Guid.NewGuid()}/{Path.GetFileName(fileName)}";

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = safeKey,
                InputStream = stream,
                AutoCloseStream = false
            };

            if (metadata != null)
            {
                if (!string.IsNullOrEmpty(metadata.ContentType))
                    request.ContentType = metadata.ContentType;
                else if (fileName.EndsWith(".nupkg", StringComparison.OrdinalIgnoreCase))
                    request.ContentType = "application/octet-stream";

                if (metadata.Tags != null)
                {
                    foreach (var tag in metadata.Tags)
                        request.TagSet.Add(new Tag { Key = tag.Key, Value = tag.Value });
                }
            }

            await _client!.PutObjectAsync(request, cancellationToken);

            _logger?.LogInformation($"[S3 Storage] Upload successful. Final Key: {safeKey}");

            return PluginResult<string>.Ok(safeKey);
        }
        catch (AmazonS3Exception ex)
        {
            _logger?.LogError($"[S3 Storage] S3 service error during upload: {fileName}", ex);
            return PluginResult<string>.Fail($"S3 error: {ex.Message}");
        }
    }

    public async Task<PluginResult<Stream>> GetAsync(string fileId, CancellationToken cancellationToken = default)
    {
        var guard = Guard();
        if (!guard.IsSuccess)
            return PluginResult<Stream>.Fail(guard.Errors);

        try
        {
            _logger?.LogInformation($"Fetching file from S3: {fileId}");

            using var s3Response = await _client!.GetObjectAsync(_bucketName, fileId, cancellationToken);

            var ms = new MemoryStream();
            await s3Response.ResponseStream.CopyToAsync(ms, cancellationToken);
            ms.Position = 0;

            return PluginResult<Stream>.Ok(ms);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return PluginResult<Stream>.Fail($"File not found: {fileId}");
        }
        catch (AmazonS3Exception ex)
        {
            _logger?.LogError($"S3 error while fetching: {fileId}", ex);
            return PluginResult<Stream>.Fail($"S3 error: {ex.Message}");
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Unexpected error while fetching: {fileId}", ex);
            return PluginResult<Stream>.Fail($"Fetch failed for '{fileId}'");
        }
    }

    public async Task<PluginResult> DeleteAsync(string fileId, CancellationToken cancellationToken = default)
    {
        var guard = Guard();
        if (!guard.IsSuccess)
            return guard;

        try
        {
            _logger?.LogInformation($"Deleting file from S3: {fileId}");
            await _client!.DeleteObjectAsync(_bucketName, fileId, cancellationToken);

            return PluginResult.Ok();
        }
        catch (AmazonS3Exception ex)
        {
            _logger?.LogError($"S3 error while deleting: {fileId}", ex);
            return PluginResult.Fail($"S3 error: {ex.Message}");
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Unexpected error while deleting: {fileId}", ex);
            return PluginResult.Fail($"Delete failed for '{fileId}'");
        }
    }

    public Task<PluginResult<string>> GetDownloadUrlAsync(string fileId, TimeSpan expires, CancellationToken cancellationToken = default)
    {
        var guard = Guard();
        if (!guard.IsSuccess)
            return Task.FromResult(PluginResult<string>.Fail(guard.Errors));

        try
        {
            var request = new GetPreSignedUrlRequest
            {
                BucketName = _bucketName,
                Key = fileId,
                Expires = DateTime.UtcNow.Add(expires)
            };

            var url = _client!.GetPreSignedURL(request);

            return Task.FromResult(PluginResult<string>.Ok(url));
        }
        catch (AmazonS3Exception ex)
        {
            _logger?.LogError($"S3 error while generating URL: {fileId}", ex);
            return Task.FromResult(PluginResult<string>.Fail($"S3 error: {ex.Message}"));
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Unexpected error while generating URL: {fileId}", ex);
            return Task.FromResult(PluginResult<string>.Fail($"Failed to generate URL for '{fileId}'"));
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