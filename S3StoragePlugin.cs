using Amazon.S3;
using Amazon.S3.Model;
using HubTo.Abstraction.Enums;
using HubTo.Abstraction.Registrars;

namespace HubTo.Plugin.S3;

public sealed class S3StoragePlugin : IStoragePlugin
{
    public string Name => "S3-Storage-Plugin";
    public PluginType Type => PluginType.Storage;

    private IAmazonS3? _client;
    private string? _bucketName;

    public Task InitializeAsync(IDictionary<string, string> settings)
    {
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

        _client = new AmazonS3Client(accessKey, secretKey, config);

        return Task.CompletedTask;
    }

    public async Task<string> SaveAsync(Stream stream, string fileName, CancellationToken ct = default)
    {
        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = fileName,
            InputStream = stream,
            AutoCloseStream = true
        };

        await _client!.PutObjectAsync(request, ct);
        return fileName;
    }

    public async Task<Stream> GetAsync(string fileId, CancellationToken ct = default)
    {
        var response = await _client!.GetObjectAsync(_bucketName, fileId, ct);
        return response.ResponseStream;
    }

    public async Task DeleteAsync(string fileId, CancellationToken ct = default)
    {
        await _client!.DeleteObjectAsync(_bucketName, fileId, ct);
    }

    public Task<string?> GetDownloadUrlAsync(string fileId, TimeSpan expires, CancellationToken ct = default)
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
}