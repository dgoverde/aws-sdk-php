<?php
namespace Aws\Glacier;

use Aws\CommandInterface;
use Aws\HashingStream;
use Aws\Multipart\AbstractUploader;
use Aws\Multipart\UploadState;
use Aws\PhpHash;
use Aws\ResultInterface;
use GuzzleHttp\Psr7;
use Psr\Http\Message\StreamableInterface as Stream;

/**
 * Encapsulates the execution of a multipart upload to Glacier.
 */
class MultipartUploader extends AbstractUploader
{
    protected function loadUploadWorkflowInfo()
    {
        $this->info = [
            'command' => [
                'initiate' => 'InitiateMultipartUpload',
                'upload'   => 'UploadMultipartPart',
                'complete' => 'CompleteMultipartUpload',
                'abort'    => 'AbortMultipartUpload',
            ],
            'defaults' => [
                'account_id' => '-',
                'vault_name' => null,
            ],
            'id' => [
                'account_id' => 'accountId',
                'vault_name' => 'vaultName',
                'upload_id'  => 'uploadId',
            ],
            'part' => [
                'min_size' => 1048576,
                'max_size' => 4294967296,
                'max_num'  => 10000,
                'param'    => 'range',
            ],
        ];
    }

    protected function loadStateFromService()
    {
        return $this->client->getPaginator('ListParts', $this->state->getId())
            ->each(function (ResultInterface $result) {
                // Get the part size from the first part in the first result.
                if (!$this->state->getPartSize()) {
                    $this->state->setPartSize($result['PartSizeInBytes']);
                }
                // Mark all the parts returned by ListParts as uploaded.
                foreach ($result['Parts'] as $part) {
                    list($rangeIndex, $rangeSize) = $this->parseRange($part['RangeInBytes']);
                    $this->state->markPartAsUploaded($rangeIndex, [
                        'size'     => $rangeSize,
                        'checksum' => $part['SHA256TreeHash'],
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

        // Calculate list of valid part sizes.
        static $validSizes;
        if (!$validSizes) {
            $validSizes = array_map(function ($n) {
                return pow(2, $n) * $this->info['part']['min_size'];
            }, range(0, 12));
        }

        // Ensure that the part size is valid.
        if (!in_array($partSize, $validSizes)) {
            throw new \InvalidArgumentException('The part_size must be a power '
                . 'of 2, in megabytes, such that 1 MB <= PART_SIZE <= 4 GB.');
        }

        return $partSize;
    }

    protected function prepareInitiateParams()
    {
        $this->config['initiate']['partSize'] = $this->state->getPartSize();
        if (isset($this->config['archive_description'])) {
            $this->config['initiate']['archiveDescription'] = $this->config['archive_description'];
        }
    }

    protected function createPart($seekable, $number)
    {
        $data = [];
        $firstByte = $this->source->tell();

        // Read from the source to create the body stream. This also
        // calculates the linear and tree hashes as the data is read.
        if ($seekable) {
            // Case 1: Stream is seekable, can make stream from new handle.
            $body = Psr7\try_fopen($this->source->getMetadata('uri'), 'r');
            $body = $this->limitPartStream(Psr7\stream_for($body));
            // Create another stream decorated with hashing streams and read
            // through it, so we can get the hash values for the part.
            $decoratedBody = $this->decorateWithHashes($body, $data);
            while (!$decoratedBody->eof()) $decoratedBody->read(1048576);
            // Seek the original source forward to the end of the range.
            $this->source->seek($this->source->tell() + $body->getSize());
        } else {
            // Case 2: Stream is not seekable, must store part in temp stream.
            $source = $this->limitPartStream($this->source);
            $source = $this->decorateWithHashes($source, $data);
            $body = Psr7\stream_for();
            Psr7\copy_to_stream($source, $body);
        }

        $body->seek(0);
        $data['body'] = $body;
        $lastByte = $this->source->tell() - 1;
        $data['range'] = "bytes {$firstByte}-{$lastByte}/*";

        return $data;
    }

    protected function handleResult(CommandInterface $command, ResultInterface $result)
    {
        list($rangeIndex, $rangeSize) = $this->parseRange($command['range']);

        $this->state->markPartAsUploaded($rangeIndex, [
            'size'     => $rangeSize,
            'checksum' => $command['checksum']
        ]);
    }

    protected function getCompleteParams()
    {
        $treeHash = new TreeHash();
        $archiveSize = 0;
        foreach ($this->state->getUploadedParts() as $part) {
            $archiveSize += $part['size'];
            $treeHash->addChecksum($part['checksum']);
        }

        return [
            'archiveSize' => $archiveSize,
            'checksum'    => bin2hex($treeHash->complete()),
        ];
    }

    /**
     * Decorates a stream with a tree AND linear sha256 hashing stream.
     *
     * @param Stream $stream Stream to decorate.
     * @param array  $data   Data bag that results are injected into.
     *
     * @return Stream
     */
    private function decorateWithHashes(Stream $stream, array &$data)
    {
        // Make sure that a tree hash is calculated.
        $stream = new HashingStream($stream, new TreeHash(),
            function ($result) use (&$data) {
                $data['checksum'] = bin2hex($result);
            }
        );

        // Make sure that a linear SHA256 hash is calculated.
        $stream = new HashingStream($stream, new PhpHash('sha256'),
            function ($result) use (&$data) {
                $data['ContentSHA256'] = bin2hex($result);
            }
        );

        return $stream;
    }

    /**
     * Parses a Glacier range string into a size and part number.
     *
     * @param string $range Glacier range string (e.g., "bytes 5-5000/*")
     *
     * @return array
     */
    private function parseRange($range)
    {
        // Strip away the prefix and suffix.
        if (strpos($range, 'bytes') !== false) {
            $range = substr($range, 6, -2);
        }

        // Split that range into it's parts.
        list($firstByte, $lastByte) = explode('-', $range);

        // Calculate and return range index and range size
        return [
            intval($firstByte / $this->state->getPartSize()) + 1,
            $lastByte - $firstByte + 1,
        ];
    }
}
