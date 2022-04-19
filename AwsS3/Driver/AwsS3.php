<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */
declare(strict_types=1);

namespace Woohoo\AwsS3\Driver;

use Generator;
use League\Flysystem\Config;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\Visibility;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\Exception\FileSystemException;
use Magento\Framework\Filesystem\DriverInterface;
use Magento\Framework\Phrase;
use Magento\RemoteStorage\Driver\Adapter\MetadataProviderInterface;
use Psr\Log\LoggerInterface;
use Magento\RemoteStorage\Driver\DriverException;
use Magento\RemoteStorage\Driver\RemoteDriverInterface;
use Magento\AwsS3\Driver\AwsS3 as AwsS3_AwsS3;


/**
 * Driver for AWS S3 IO operations.
 *
 * @SuppressWarnings(PHPMD.ExcessiveClassComplexity)
 */
class AwsS3 extends AwsS3_AwsS3
{
    private const CONFIG = ['ACL' => 'public-read', 'visibility' => Visibility::PRIVATE];

    /**
     * @var FilesystemAdapter
     */
    private $adapter;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var array
     */
    private $streams = [];

    /**
     * @var string
     */
    private $objectUrl;

    /**
     * @var MetadataProviderInterface
     */
    private $metadataProvider;

    /**
     * @param FilesystemAdapter $adapter
     * @param LoggerInterface $logger
     * @param string $objectUrl
     * @param MetadataProviderInterface|null $metadataProvider
     */
    public function __construct(
        FilesystemAdapter $adapter,
        LoggerInterface $logger,
        string $objectUrl,
        MetadataProviderInterface $metadataProvider = null
    ) {
        $this->adapter = $adapter;
        $this->logger = $logger;
        $this->objectUrl = $objectUrl;
        $this->metadataProvider = $metadataProvider ??
            ObjectManager::getInstance()->get(MetadataProviderInterface::class);

        parent::__construct($adapter,$logger,$objectUrl,$metadataProvider);    
    }

    
    /**
     * Create directory recursively.
     *
     * @param string $path
     * @return bool
     * @throws FileSystemException
     */
    private function createDirectoryRecursively(string $path): bool
    {
        $path = $this->normalizeRelativePath($path);
        //phpcs:ignore Magento2.Functions.DiscouragedFunction
        $parentDir = dirname($path);

        while (!$this->isDirectory($parentDir)) {
            $this->createDirectoryRecursively($parentDir);
        }

        if (!$this->isDirectory($path)) {

            try {
                $this->adapter->createDirectory($this->fixPath($path), new Config(self::CONFIG));
            } catch (\League\Flysystem\FilesystemException $e) {
                $this->logger->error($e->getMessage());
                return false;
            }
        }

        return true;
    }

    /**
     * @inheritDoc
     */
    public function copy($source, $destination, DriverInterface $targetDriver = null): bool
    {
        try {
            $this->adapter->copy(
                $this->normalizeRelativePath($source, true),
                $this->normalizeRelativePath($destination, true),
                new Config(self::CONFIG)
            );
        } catch (\League\Flysystem\FilesystemException $e) {
            $this->logger->error($e->getMessage());
            return false;
        }
        return true;
    }

   

    

    /**
     * @inheritDoc
     */
    public function filePutContents($path, $content, $mode = null): int
    {
        $path = $this->normalizeRelativePath($path, true);
        $config = self::CONFIG;

        if (false !== ($imageSize = @getimagesizefromstring($content))) {
            $config['Metadata'] = [
                'image-width' => $imageSize[0],
                'image-height' => $imageSize[1]
            ];
        }

        try {
            $this->adapter->write($path, $content, new Config($config));
            return $this->adapter->fileSize($path)->fileSize();
        } catch (\League\Flysystem\FilesystemException | UnableToRetrieveMetadata $e) {
            $this->logger->error($e->getMessage());
            return 0;
        }
    }
    

    /**
     * Resolves relative path.
     *
     * @param string $path Absolute path
     * @param bool $fixPath
     * @return string Relative path
     */
    private function normalizeRelativePath(string $path, bool $fixPath = false): string
    {
        $relativePath = str_replace($this->normalizeAbsolutePath(''), '', $path);

        if ($fixPath) {
            $relativePath = $this->fixPath($relativePath);
        }

        return $relativePath;
    }

    /**
     * Resolves absolute path.
     *
     * @param string $path Relative path
     * @return string Absolute path
     */
    private function normalizeAbsolutePath(string $path): string
    {
        $path = str_replace($this->getObjectUrl(''), '', $path);

        return $this->getObjectUrl($path);
    }

    /**
     * Retrieves object URL from cache.
     *
     * @param string $path
     * @return string
     */
    private function getObjectUrl(string $path): string
    {
        return $this->objectUrl . ltrim($path, '/');
    }


    

    /**
     * @inheritDoc
     */
    public function rename($oldPath, $newPath, DriverInterface $targetDriver = null): bool
    {
        try {
            $this->adapter->move(
                $this->normalizeRelativePath($oldPath, true),
                $this->normalizeRelativePath($newPath, true),
                new Config(self::CONFIG)
            );
        } catch (\League\Flysystem\FilesystemException $e) {
            $this->logger->error($e->getMessage());
            return false;
        }
        return true;
    }


    /**
     * Emulate php glob function for AWS S3 storage
     *
     * @param string $pattern
     * @return Generator
     * @throws FileSystemException
     */
    private function glob(string $pattern): Generator
    {
        $patternFound = preg_match('(\*|\?|\[.+\])', $pattern, $parentPattern, PREG_OFFSET_CAPTURE);

        if ($patternFound) {
            // phpcs:ignore Magento2.Functions.DiscouragedFunction
            $parentDirectory = dirname(substr($pattern, 0, $parentPattern[0][1] + 1));
            $leftover = substr($pattern, $parentPattern[0][1]);
            $index = strpos($leftover, '/');
            $searchPattern = $this->getSearchPattern($pattern, $parentPattern, $parentDirectory, $index);

            if ($this->isDirectory($parentDirectory)) {
                yield from $this->getDirectoryContent($parentDirectory, $searchPattern, $leftover, $index);
            }
        } elseif ($this->isExists($pattern)) {
            yield $this->normalizeAbsolutePath($pattern);
        }
    }

    

    /**
     * @inheritDoc
     */
    public function fileClose($resource): bool
    {
        //phpcs:disable
        $resourcePath = stream_get_meta_data($resource)['uri'];
        //phpcs:enable

        foreach ($this->streams as $path => $stream) {
            //phpcs:disable
            if (stream_get_meta_data($stream)['uri'] === $resourcePath) {
                $this->adapter->writeStream($path, $resource, new Config(self::CONFIG));

                // Remove path from streams after
                unset($this->streams[$path]);

                return fclose($stream);
            }
        }

        return false;
    }

    

    /**
     * Removes slashes in path.
     *
     * @param string $path
     * @return string
     */
    private function fixPath(string $path): string
    {
        return trim($path, '/');
    }



    /**
     * Get search pattern for directory
     *
     * @param string $pattern
     * @param array $parentPattern
     * @param string $parentDirectory
     * @param int|bool $index
     * @return string
     */
    private function getSearchPattern(string $pattern, array $parentPattern, string $parentDirectory, $index): string
    {
        $parentLength = strlen($parentDirectory);
        if ($index !== false) {
            $searchPattern = substr(
                $pattern,
                $parentLength + 1,
                $parentPattern[0][1] - $parentLength + $index - 1
            );
        } else {
            $searchPattern = substr($pattern, $parentLength + 1);
        }

        $replacement = [
            '/\*/' => '.*',
            '/\?/' => '.',
            '/\//' => '\/'
        ];

        return preg_replace(array_keys($replacement), array_values($replacement), $searchPattern);
    }

    /**
     * Get directory content by given search pattern
     *
     * @param string $parentDirectory
     * @param string $searchPattern
     * @param string $leftover
     * @param int|bool $index
     * @return Generator
     * @throws FileSystemException
     */
    private function getDirectoryContent(
        string $parentDirectory,
        string $searchPattern,
        string $leftover,
        $index
    ): Generator {
        $items = $this->readDirectory($parentDirectory);
        $directoryContent = [];
        foreach ($items as $item) {
            if (preg_match('/' . $searchPattern . '$/', $item)
                // phpcs:ignore Magento2.Functions.DiscouragedFunction
                && strpos(basename($item), '.') !== 0) {
                if ($index === false || strlen($leftover) === $index + 1) {
                    yield $this->normalizeAbsolutePath(
                        $this->isDirectory($item) ? rtrim($item, '/') . '/' : $item
                    );
                } elseif (strlen($leftover) > $index + 1) {
                    yield from $this->glob("{$parentDirectory}/{$item}" . substr($leftover, $index));
                }
            }
        }

        return $directoryContent;
    }
}
