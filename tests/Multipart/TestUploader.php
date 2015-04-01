<?php
namespace Aws\Test\Multipart;

use Aws\CommandInterface;
use Aws\Multipart\UploadState;
use Aws\Multipart\AbstractUploader;
use Aws\ResultInterface;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7;

/**
 * Concrete UploadBuilder for the purposes of the following test.
 */
class TestUploader extends AbstractUploader
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
                'min_size' => 2,
                'max_size' => 10,
                'max_num'  => 10,
                'param'    => 'PartNumber',
            ],
        ];
    }

    protected function loadStateFromService()
    {
        $state = new UploadState($this->state->getId());
        $state->setPartSize(2);
        $state->setStatus(UploadState::INITIATED);
        $state->markPartAsUploaded(1, ['PartNumber' => 1, 'ETag' => 'A']);
        $state->markPartAsUploaded(2, ['PartNumber' => 2, 'ETag' => 'B']);

        return new FulfilledPromise($state);
    }

    protected function determinePartSize()
    {
        return $this->config['part_size'] ?: $this->info['part']['min_size'];
    }

    protected function prepareInitiateParams()
    {
    }

    protected function createPart($seekable, $number)
    {
        if ($seekable) {
            $body = Psr7\stream_for(fopen($this->source->getMetadata('uri'), 'r'));
            $body = $this->limitPartStream($body);
        } else {
            $body = Psr7\stream_for($this->source->read($this->state->getPartSize()));
        }

        return [
            'PartNumber' => $number,
            'Body'       => $body,
        ];
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
        return [
            'MultipartUpload' => [
                'Parts' => $this->state->getUploadedParts()
            ]
        ];
    }
}