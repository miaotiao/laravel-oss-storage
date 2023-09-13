<?php
/**
 * Created by jacob.
 * Date: 2016/5/19 0019
 * Time: 下午 17:07
 */

namespace Jacobcyl\AliOSS;

use League\Flysystem\Config;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\Visibility;
use OSS\Core\OssException;
use OSS\OssClient;

class AliOssAdapter implements FilesystemAdapter
{
    /**
     * @var array
     */
    protected static array $resultMap = [
        'Body' => 'raw_contents',
        'Content-Length' => 'size',
        'ContentType' => 'mimetype',
        'Size' => 'size',
        'StorageClass' => 'storage_class',
    ];

    /**
     * @var array
     */
    protected static array $metaOptions = [
        'CacheControl',
        'Expires',
        'ServerSideEncryption',
        'Metadata',
        'ACL',
        'ContentType',
        'ContentDisposition',
        'ContentLanguage',
        'ContentEncoding',
    ];

    protected static array $metaMap = [
        'CacheControl' => 'Cache-Control',
        'Expires' => 'Expires',
        'ServerSideEncryption' => 'x-oss-server-side-encryption',
        'Metadata' => 'x-oss-metadata-directive',
        'ACL' => 'x-oss-object-acl',
        'ContentType' => 'Content-Type',
        'ContentDisposition' => 'Content-Disposition',
        'ContentLanguage' => 'response-content-language',
        'ContentEncoding' => 'Content-Encoding',
    ];

    //Aliyun OSS Client OssClient
    protected OssClient $client;
    //bucket name
    protected string $bucket;

    protected string $prefix;

    protected string $endPoint;

    protected string $cdnDomain;

    protected bool $ssl;

    protected bool $isCname;

    //配置
    protected array $options = [
        'Multipart' => 128
    ];


    /**
     * AliOssAdapter constructor.
     *
     * @param OssClient $client
     * @param string $bucket
     * @param string $endPoint
     * @param bool $ssl
     * @param bool $isCname
     * @param string $cdnDomain
     * @param string|null $prefix
     * @param array $options
     */
    public function __construct(
        OssClient $client,
        string    $bucket,
        string    $endPoint,
        bool      $ssl,
        bool      $isCname = false,
        string    $cdnDomain,
        ?string   $prefix = null,
        array     $options = []
    )
    {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->prefix = $prefix;
        $this->endPoint = $endPoint;
        $this->ssl = $ssl;
        $this->isCname = $isCname;
        $this->cdnDomain = $cdnDomain;
        $this->options = array_merge($this->options, $options);
    }

    /**
     * Get the OssClient bucket.
     *
     * @return string
     */
    public function getBucket(): string
    {
        return $this->bucket;
    }

    /**
     * Get the OSSClient instance.
     *
     * @return OssClient
     */
    public function getClient(): OssClient
    {
        return $this->client;
    }

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     * @return void
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $location = $this->applyPathPrefix($path);
        $options = $this->getOptions($this->options, $config);

//        if (!isset($options[OssClient::OSS_LENGTH])) {
//            $options[OssClient::OSS_LENGTH] = Util::contentSize($contents);
//        }
//        if (!isset($options[OssClient::OSS_CONTENT_TYPE])) {
//            $options[OssClient::OSS_CONTENT_TYPE] = Util::guessMimeType($path, $contents);
//        }

        $this->client->putObject($this->bucket, $location, $contents, $options);
    }

    /**
     * Write a new file using a stream.
     *
     * @param string $path
     * @param resource $contents
     * @param Config $config Config object
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->write($path, stream_get_contents($contents), $config);
    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config)
    {
        if (!$config->has('visibility') && !$config->has('ACL')) {
            $config->set(static::$metaMap['ACL'], $this->getObjectACL($path));
        }
        // $this->delete($path);
        return $this->write($path, $contents, $config);
    }

    /**
     * Update a file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, Config $config)
    {
        $contents = stream_get_contents($resource);
        return $this->update($path, $contents, $config);
    }

    /**
     * @throws OssException
     */
    public function copy($source, $destination, Config $config): void
    {
        $object = $this->applyPathPrefix($source);
        $newObject = $this->applyPathPrefix($destination);

        $this->client->copyObject($this->bucket, $object, $this->bucket, $newObject, $this->getOptions([], $config));
    }

    public function delete(string $path): void
    {
        $object = $this->applyPathPrefix($path);
        $this->client->deleteObject($this->bucket, $object);
    }

    public function deleteDir($dirname)
    {
        $dirname = rtrim($this->applyPathPrefix($dirname), '/') . '/';
        $dirObjects = $this->listDirObjects($dirname, true);

        if (count($dirObjects['objects']) > 0) {

            foreach ($dirObjects['objects'] as $object) {
                $objects[] = $object['Key'];
            }

            try {
                $this->client->deleteObjects($this->bucket, $objects);
            } catch (OssException $e) {
                $this->logErr(__FUNCTION__, $e);
                return false;
            }

        }

        try {
            $this->client->deleteObject($this->bucket, $dirname);
        } catch (OssException $e) {
            $this->logErr(__FUNCTION__, $e);
            return false;
        }

        return true;
    }

    /**
     * 列举文件夹内文件列表；可递归获取子文件夹；
     * @param string $dirname 目录
     * @param bool $recursive 是否递归
     * @return mixed
     * @throws OssException
     */
    public function listDirObjects($dirname = '', $recursive = false)
    {
        $delimiter = '/';
        $nextMarker = '';
        $maxkeys = 1000;

        //存储结果
        $result = [];

        while (true) {
            $options = [
                'delimiter' => $delimiter,
                'prefix' => $dirname,
                'max-keys' => $maxkeys,
                'marker' => $nextMarker,
            ];

            try {
                $listObjectInfo = $this->client->listObjects($this->bucket, $options);
            } catch (OssException $e) {
                $this->logErr(__FUNCTION__, $e);
                // return false;
                throw $e;
            }

            $nextMarker = $listObjectInfo->getNextMarker(); // 得到nextMarker，从上一次listObjects读到的最后一个文件的下一个文件开始继续获取文件列表
            $objectList = $listObjectInfo->getObjectList(); // 文件列表
            $prefixList = $listObjectInfo->getPrefixList(); // 目录列表

            if (!empty($objectList)) {
                foreach ($objectList as $objectInfo) {

                    $object['Prefix'] = $dirname;
                    $object['Key'] = $objectInfo->getKey();
                    $object['LastModified'] = $objectInfo->getLastModified();
                    $object['eTag'] = $objectInfo->getETag();
                    $object['Type'] = $objectInfo->getType();
                    $object['Size'] = $objectInfo->getSize();
                    $object['StorageClass'] = $objectInfo->getStorageClass();

                    $result['objects'][] = $object;
                }
            } else {
                $result["objects"] = [];
            }

            if (!empty($prefixList)) {
                foreach ($prefixList as $prefixInfo) {
                    $result['prefix'][] = $prefixInfo->getPrefix();
                }
            } else {
                $result['prefix'] = [];
            }

            //递归查询子目录所有文件
            if ($recursive) {
                foreach ($result['prefix'] as $pfix) {
                    $next = $this->listDirObjects($pfix, $recursive);
                    $result["objects"] = array_merge($result['objects'], $next["objects"]);
                }
            }

            //没有更多结果了
            if ($nextMarker === '') {
                break;
            }
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createDir($dirname, Config $config)
    {
        $object = $this->applyPathPrefix($dirname);
        $options = $this->getOptionsFromConfig($config);

        try {
            $this->client->createObjectDir($this->bucket, $object, $options);
        } catch (OssException $e) {
            $this->logErr(__FUNCTION__, $e);
            return false;
        }

        return ['path' => $dirname, 'type' => 'dir'];
    }

    /**
     * @throws OssException
     */
    public function setVisibility(string $path, string $visibility): void
    {
        $object = $this->applyPathPrefix($path);
        $acl = ($visibility === Visibility::PUBLIC) ? OssClient::OSS_ACL_TYPE_PUBLIC_READ : OssClient::OSS_ACL_TYPE_PRIVATE;

        $this->client->putObjectAcl($this->bucket, $object, $acl);
    }

    /**
     * {@inheritdoc}
     */
    public function has($path)
    {
        $object = $this->applyPathPrefix($path);

        return $this->client->doesObjectExist($this->bucket, $object);
    }

    /**
     * {@inheritdoc}
     */
    public function read($path): string
    {
        $object = $this->applyPathPrefix($path);

        return $this->client->getObject($this->bucket, $object);
    }

    /**
     * {@inheritdoc}
     */
    public function readStream($path)
    {
        $result = $this->readObject($path);
        $result['stream'] = $result['raw_contents'];
        rewind($result['stream']);
        // Ensure the EntityBody object destruction doesn't close the stream
        $result['raw_contents']->detachStream();
        unset($result['raw_contents']);

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function listContents($path = '', $deep = false): iterable
    {
        $dirObjects = $this->listDirObjects($path, true);
        $contents = $dirObjects["objects"];

        $result = array_map([$this, 'normalizeResponse'], $contents);
        $result = array_filter($result, function ($value) {
            return $value['path'] !== false;
        });

        return Util::emulateDirectories($result);
    }

    /**
     * {@inheritdoc}
     */
    public function getMetadata($path)
    {
        $object = $this->applyPathPrefix($path);

        try {
            $objectMeta = $this->client->getObjectMeta($this->bucket, $object);
        } catch (OssException $e) {
            $this->logErr(__FUNCTION__, $e);
            return false;
        }

        return $objectMeta;
    }

    /**
     * {@inheritdoc}
     */
    public function getSize($path)
    {
        $object = $this->getMetadata($path);
        $object['size'] = $object['content-length'];
        return $object;
    }

    /**
     * {@inheritdoc}
     */
    public function getMimetype($path)
    {
        if ($object = $this->getMetadata($path))
            $object['mimetype'] = $object['content-type'];
        return $object;
    }

    /**
     * {@inheritdoc}
     */
    public function getTimestamp($path)
    {
        if ($object = $this->getMetadata($path))
            $object['timestamp'] = strtotime($object['last-modified']);
        return $object;
    }

    /**
     * The ACL visibility.
     *
     * @param string $path
     *
     * @return string
     */
    protected function getObjectACL($path)
    {
        $metadata = $this->getVisibility($path);

        return $metadata['visibility'] === Visibility::PUBLIC ? OssClient::OSS_ACL_TYPE_PUBLIC_READ : OssClient::OSS_ACL_TYPE_PRIVATE;
    }

    /**
     * Get options for a OSS call. done
     *
     * @param array $options
     * @param Config|null $config
     * @return array OSS options
     */
    protected function getOptions(array $options = [], Config $config = null): array
    {
        $options = array_merge($this->options, $options);

        if ($config) {
            $options = array_merge($options, $this->getOptionsFromConfig($config));
        }

        return array(OssClient::OSS_HEADERS => $options);
    }

    /**
     * Retrieve options from a Config instance. done
     *
     * @param Config $config
     *
     * @return array
     */
    protected function getOptionsFromConfig(Config $config): array
    {
        $options = [];

        foreach (static::$metaOptions as $option) {
            if (!$config->has($option)) {
                continue;
            }
            $options[static::$metaMap[$option]] = $config->get($option);
        }

        if ($visibility = $config->get('visibility')) {
            // For local reference
            // $options['visibility'] = $visibility;
            // For external reference
            $options['x-oss-object-acl'] = $visibility === Visibility::PUBLIC ? OssClient::OSS_ACL_TYPE_PUBLIC_READ : OssClient::OSS_ACL_TYPE_PRIVATE;
        }

        if ($mimetype = $config->get('mimetype')) {
            // For local reference
            // $options['mimetype'] = $mimetype;
            // For external reference
            $options['Content-Type'] = $mimetype;
        }

        return $options;
    }

    public function fileExists(string $path): bool
    {
        return $this->client->doesObjectExist($this->bucket, $this->applyPathPrefix($path));
    }

    public function directoryExists(string $path): bool
    {
        return $this->client->doesObjectExist($this->bucket, $this->applyPathPrefix($path));
    }

    public function deleteDirectory(string $path): void
    {
        $options = [
            OssClient::OSS_MARKER => null,
            OssClient::OSS_PREFIX => $this->applyPathPrefix($path),
        ];
        while (true) {
            $results = $this->client->listObjectsV2($this->bucket, $options);

            $objects = [];
            if (count($results->getObjectList()) > 0) {
                foreach ($results->getObjectList() as $info) {
                    $objects[] = $info->getKey();
                }
                $this->client->deleteObjects($this->bucket, $objects);
            }
        }
    }

    public function createDirectory(string $path, Config $config): void
    {
        $this->client->createObjectDir($this->bucket, $this->applyPathPrefix($path), $this->getOptions([], $config));
    }

    /**
     * @throws OssException
     */
    public function visibility(string $path): FileAttributes
    {
        $object = $this->applyPathPrefix($path);


        $acl = $this->client->getObjectAcl($this->bucket, $object);

        return new FileAttributes($object, visibility: $acl);
    }

    public function mimeType(string $path): FileAttributes
    {
        $object = $this->applyPathPrefix($path);
        $meta = $this->client->getObjectMeta($this->bucket, $object);
        return new FileAttributes($object);
    }

    public function lastModified(string $path): FileAttributes
    {
        $object = $this->applyPathPrefix($path);
        $meta = $this->client->getObjectMeta($this->bucket, $object);
        return new FileAttributes($object);
    }

    public function fileSize(string $path): FileAttributes
    {
        $object = $this->applyPathPrefix($path);
        $meta = $this->client->getObjectMeta($this->bucket, $object);
        return new FileAttributes($object);
    }

    /**
     * @throws OssException
     * @throws FilesystemException
     */
    public function move(string $source, string $destination, Config $config): void
    {
        $sourcePath = $this->applyPathPrefix($source);
        $this->client->copyObject($this->bucket, $sourcePath, $this->bucket, $this->applyPathPrefix($destination), $this->getOptions([], $config));
        $this->delete($sourcePath);
    }

    private function applyPathPrefix(string $path): string
    {
        return trim($this->prefix, '/') . '/' . $path;
    }
}
