using Amazon.S3;
using Amazon.S3.Model;
using HubTo.Abstraction.Enums;
using HubTo.Abstraction.Logging;
using HubTo.Abstraction.Registrars;

namespace HubTo.Plugin.S3;

public sealed class S3StoragePlugin : IStoragePlugin
{
    public string Name => "S3-Storage-Plugin";
    public PluginType Type => PluginType.Storage;

    private IAmazonS3? _client;
    private string? _bucketName;
    private IHubToLogger? _logger;

    public Task InitializeAsync(
        IDictionary<string, string> settings,
        IHubToLogger logger,
        CancellationToken cancellationToken = default)
    {
        _logger = logger;
        _logger.LogInformation("Initializing S3 Storage Plugin...");

        settings.TryGetValue("ServiceUrl", out var serviceUrl);
        settings.TryGetValue("AccessKey", out var accessKey);
        settings.TryGetValue("SecretKey", out var secretKey);
        settings.TryGetValue("Region", out var region);
        settings.TryGetValue("BucketName", out _bucketName);

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
            _logger.LogError("Failed to initialize S3 Client", ex);
            throw;
        }

        return Task.CompletedTask;
    }

    public async Task<string> SaveAsync(Stream stream, string fileName, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation($"Uploading file to S3: {fileName}");

        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = fileName,
            InputStream = stream,
            AutoCloseStream = true
        };

        await _client!.PutObjectAsync(request, cancellationToken);
        return fileName;
    }

    public async Task<Stream> GetAsync(string fileId, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation($"Fetching file from S3: {fileId}");
        var response = await _client!.GetObjectAsync(_bucketName, fileId, cancellationToken);
        return response.ResponseStream;
    }

    public async Task DeleteAsync(string fileId, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation($"Deleting file from S3: {fileId}");
        await _client!.DeleteObjectAsync(_bucketName, fileId, cancellationToken);
    }

    public Task<string?> GetDownloadUrlAsync(string fileId, TimeSpan expires, CancellationToken cancellationToken = default)
    {
        var request = new GetPreSignedUrlRequest
        {
            BucketName = _bucketName,
            Key = fileId,
            Expires = DateTime.UtcNow.Add(expires)
        };

        return Task.FromResult<string?>(_client!.GetPreSignedURL(request));
    }

    public bool CanHandle(string path, string method) => false;

    public Task ShutdownAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Shutting down S3 Storage Plugin...");
        _client?.Dispose();
        return Task.CompletedTask;
    }
}