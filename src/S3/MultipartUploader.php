<?php
namespace Aws\S3;

use Aws\CommandInterface;
use Aws\HashingStream;
use Aws\Multipart\AbstractUploader;
use Aws\Multipart\UploadState;
use Aws\PhpHash;
use Aws\ResultInterface;
use GuzzleHttp\Psr7;
use Psr\Http\Message\StreamableInterface as Stream;

/**
 * Encapsulates the execution of a multipart upload to S3 or Glacier.
 */
class MultipartUploader extends AbstractUploader
{
    protected function loadUploadWorkflowInfo()
    {
        $this->info = [
            'command' => [
                'initiate' => 'CreateMultipartUpload',
                'upload'   => 'UploadPart',
                'complete' => 'CompleteMultipartUpload',
                'abort'    => 'AbortMultipartUpload',
            ],
            'defaults' => [
                'bucket' => null,
                'key'    => null,
            ],
            'id' => [
                'bucket'    => 'Bucket',
                'key'       => 'Key',
                'upload_id' => 'UploadId',
            ],
            'part' => [
                'min_size' => 5242880,
                'max_size' => 5368709120,
                'max_num'  => 10000,
                'param'    => 'PartNumber',
            ],
        ];
    }

    protected function loadStateFromService()
    {
        return $this->client->getPaginator('ListParts', $this->state->getId())
            ->each(function (ResultInterface $result) {
                // Get the part size from the first part in the first result.
                if (!$this->state->getPartSize()) {
                    $this->state->setPartSize($result->search('Parts[0].Size'));
                }
                // Mark all the parts returned by ListParts as uploaded.
                foreach ($result['Parts'] as $part) {
                    $this->state->markPartAsUploaded($part['PartNumber'], [
                        'PartNumber' => $part['PartNumber'],
                        'ETag'       => $part['ETag']
                    ]);
                }
            })
            ->then(function () {
                // Set the state's status, then return it to fulfill the promise.
                $this->state->setStatus(UploadState::INITIATED);
                return $this->state;
            });
    }

    protected function determinePartSize()
    {
        // Make sure the part size is set.
        $partSize = $this->config['part_size'] ?: $this->info['part']['min_size'];

        // Adjust the part size to be larger for known, x-large uploads.
        if ($sourceSize = $this->source->getSize()) {
            $partSize = (int) max(
                $partSize,
                ceil($sourceSize / $this->info['part']['max_num'])
            );
        }

        // Ensure that the part size follows the rules: 5 MB <= size <= 5 GB.
        if ($partSize < $this->info['part']['min_size']
            || $partSize > $this->info['part']['max_size']
        ) {
            throw new \InvalidArgumentException('The part size must be no less '
                . 'than 5 MB and no greater than 5 GB.');
        }

        return $partSize;
    }

    protected function prepareInitiateParams()
    {
        // Set the content type, if not specified, and can be detected.
        if (!isset($this->config['initiate']['ContentType'])
            && ($uri = $this->source->getMetadata('uri'))
        ) {
            $mimeType = Psr7\mimetype_from_filename($uri) ?: 'application/octet-stream';
            $this->config['initiate']['ContentType'] = $mimeType;
        }
    }

    protected function createPart($seekable, $number)
    {
        // Initialize the array of part data that will be returned.
        $data = ['PartNumber' => $number];

        // Read from the source to create the body stream.
        if ($seekable) {
            // Case 1: Source is seekable, use lazy stream to defer work.
            $body = $this->limitPartStream(
                new Psr7\LazyOpenStream($this->source->getMetadata('uri'), 'r')
            );
        } else {
            // Case 2: Stream is not seekable; must store in temp stream.
            $source = $this->limitPartStream($this->source);
            $source = $this->decorateWithHashes($source,
                function ($result, $type) use (&$data) {
                    $data['Content' . strtoupper($type)] = $result;
                }
            );
            $body = Psr7\stream_for();
            Psr7\copy_to_stream($source, $body);
            $data['ContentLength'] = $body->getSize();
        }

        $body->seek(0);
        $data['Body'] = $body;

        return $data;
    }

    protected function handleResult(CommandInterface $command, ResultInterface $result)
    {
        $this->state->markPartAsUploaded($command['PartNumber'], [
            'PartNumber' => $command['PartNumber'],
            'ETag'       => $result['ETag']
        ]);
    }

    protected function getCompleteParams()
    {
        return ['MultipartUpload' => [
            'Parts' => $this->state->getUploadedParts()
        ]];
    }

    /**
     * Decorates a stream with a md5/sha256 linear hashing stream if needed.
     *
     * S3 does not typically require content hashes (unless using Signature V4),
     * but they can be used to ensure the message integrity of the upload.
     * When using non-seekable/remote streams, we must do the work of reading
     * through the body to calculate parts. In this case, we can wrap the parts'
     * body streams with a hashing stream decorator to calculate the hashes at
     * the same time, instead of having to buffer the stream to disk and re-read
     * the stream later.
     *
     * @param Stream   $stream   Stream to decorate.
     * @param callable $complete Callback to execute for the hash result.
     *
     * @return Stream
     */
    private function decorateWithHashes(Stream $stream, callable $complete)
    {
        // Determine if the checksum needs to be calculated.
        if ($this->client->getConfig('signature_version') == 'v4') {
            $type = 'sha256';
        } elseif ($this->client->getConfig('calculate_md5')) {
            $type = 'md5';
        } else {
            return $stream;
        }

        // Decorate source with a hashing stream
        $hash = new PhpHash($type, ['base64' => true]);
        return new HashingStream($stream, $hash,
            function ($result) use ($type, $complete) {
                return $complete($result, $type);
            }
        );
    }
}
