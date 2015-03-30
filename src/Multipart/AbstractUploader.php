<?php
namespace Aws\Multipart;

use Aws\AwsClientInterface as Client;
use Aws\CommandInterface;
use Aws\CommandPool;
use Aws\Exception\AwsException;
use Aws\Exception\MultipartUploadException;
use Aws\Result;
use Aws\ResultInterface;
use GuzzleHttp\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7;
use InvalidArgumentException as IAE;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\StreamableInterface as Stream;

/**
 * Encapsulates the execution of a multipart upload to S3 or Glacier.
 */
abstract class AbstractUploader
{
    /** @var Client Client used for the upload. */
    protected $client;

    /** @var array Configuration used to perform the upload. */
    protected $config = [
        'part_size'   => null,
        'upload_id'   => null,
        'state'       => null,
        'concurrency' => 3,
        'before_part' => null,
        'initiate'    => [],
        'upload'      => [],
        'complete'    => [],
        'abort'       => [],
    ];

    /** @var array Service-specific information about the upload workflow. */
    protected $info = [];

    /** @var PromiseInterface Promise that represents the multipart upload. */
    protected $promise;

    /** @var Stream Source of the data to be uploaded. */
    protected $source;

    /** @var UploadState State used to manage the upload. */
    protected $state;

    /**
     * Construct a new transfer object
     *
     * @param Client $client
     * @param mixed  $source
     * @param array  $config
     */
    public function __construct(Client $client, $source, array $config = [])
    {
        $this->loadUploadWorkflowInfo();
        $this->config += $config + $this->info['defaults'];
        $this->client = $client;
        $this->prepareSource($source);
        $this->prepareState();
    }

    /**
     * Returns the current state of the upload
     *
     * @return UploadState
     */
    public function getState()
    {
        if ($this->state instanceof Promise\PromiseInterface) {
            $this->state = $this->state->wait();
        }

        return $this->state;
    }

    /**
     * Upload the source using multipart upload operations.
     *
     * @return Result The result of the CompleteMultipartUpload operation.
     * @throws \LogicException if the upload is already complete or aborted.
     * @throws MultipartUploadException if an upload operation fails.
     */
    public function upload()
    {
        return $this->uploadAsync()->wait();
    }

    /**
     * Abort the multipart upload.
     *
     * @return Result
     * @throws \LogicException if the upload has not been initiated yet.
     * @throws MultipartUploadException if the abort operation fails.
     */
    public function abort()
    {
        return $this->abortAsync()->wait();
    }

    /**
     * Upload the source asynchronously using multipart upload operations.
     *
     * @return PromiseInterface
     */
    public function uploadAsync()
    {
        if ($this->promise) {
            return $this->promise;
        }

        return $this->promise = Promise\coroutine(function () {
            // Determine the state of the upload
            if ($this->state instanceof Promise\PromiseInterface) {
                $this->state = (yield $this->state);
            }

            // Initiate the upload
            if ($this->state->isCompleted() || $this->state->isAborted()) {
                throw new \LogicException('This multipart upload has already '
                    . 'been completed or aborted.'
                );
            } elseif (!$this->state->isInitiated()) {
                $this->prepareInitiateParams();
                $result = (yield $this->client->executeAsync(
                    $this->createCommand('initiate')
                ));
                $this->state->setUploadId(
                    $this->info['id']['upload_id'],
                    $result[$this->info['id']['upload_id']]
                );
                $this->state->setStatus(UploadState::INITIATED);
            }

            // Create a command pool from a generator that yields UploadPart
            // commands for each upload part.
            $commands = new CommandPool(
                $this->client,
                $this->getUploadCommands($this->getResultHandler($errors)),
                [
                    'concurrency' => $this->config['concurrency'],
                    'before'      => $this->config['before_part'],
                ]
            );

            // Execute the pool of commands concurrently, and process errors.
            yield $commands->promise();
            if ($errors) {
                throw new MultipartUploadException($this->state, $errors);
            }

            // Complete the multipart upload.
            yield $this->client->executeAsync(
                $this->createCommand('complete', $this->getCompleteParams())
            );
            $this->state->setStatus(UploadState::COMPLETED);
        })->then(null, function (\Exception $e) {
            // Throw errors from the operations as a specific Multipart error.
            if ($e instanceof AwsException) {
                $e = new MultipartUploadException($this->state, $e);
            }
            throw $e;
        });
    }

    /**
     * Abort the multipart upload asynchronously.
     *
     * @return PromiseInterface
     */
    public function abortAsync()
    {
        return Promise\coroutine(function () {
            if ($this->state instanceof Promise\PromiseInterface) {
                $this->state = (yield $this->state);
            }

            if ($this->state->isInitiated()) {
                yield $this->client->executeAsync($this->createCommand('abort'));
                $this->state->setStatus(UploadState::ABORTED);
            } else {
                throw new \LogicException('This multipart upload has already '
                    . 'been completed or aborted, or has not been initiated.'
                );
            }
        });
    }

    /**
     * Create a stream for a part that starts at the current position and
     * has a length of the upload part size (or less with the final part).
     *
     * @param Stream $stream
     *
     * @return Psr7\LimitStream
     */
    protected function limitPartStream(Stream $stream)
    {
        // Limit what is read from the stream to the part size.
        return new Psr7\LimitStream(
            $stream,
            $this->state->getPartSize(),
            $this->source->tell()
        );
    }

    /**
     * Provides service-specific information about the multipart upload
     * workflow.
     *
     * This array of data should include the keys: 'command', 'defaults', 'id',
     * and 'part'.
     *
     * @return array
     */
    abstract protected function loadUploadWorkflowInfo();

    /**
     * @return PromiseInterface Promise fulfilled with an UploadState.
     */
    abstract protected function loadStateFromService();

    /**
     * Determines the part size to use for upload parts.
     *
     * Examines the provided partSize value and the source to determine the
     * best possible part size.
     *
     * @throws \InvalidArgumentException if the part size is invalid.
     *
     * @return int
     */
    abstract protected function determinePartSize();

    /**
     * Performs service-specific logic to prepare parameters for initiate.
     */
    abstract protected function prepareInitiateParams();

    /**
     * Generates the parameters for an upload part by analyzing a range of the
     * source starting from the current offset up to the part size.
     *
     * @param bool $seekable
     * @param int  $number
     *
     * @return array
     */
    abstract protected function createPart($seekable, $number);

    /**
     * Uses information from the Command and Result to determine which part was
     * uploaded and mark it as uploaded in the upload's state.
     *
     * @param CommandInterface $command
     * @param ResultInterface  $result
     */
    abstract protected function handleResult(
        CommandInterface $command,
        ResultInterface $result
    );

    /**
     * Prepares the command parameters for completing the multipart upload.
     *
     * @return array
     */
    abstract protected function getCompleteParams();

    /**
     * Based on the config and workflow info, creates an UploadState object.
     *
     * 1. If configured with an instance of UploadState, that object is used.
     * 2. If configured with an `'upload_id'`, then service-specific logic is
     *    used to load the upload's state from the service. The state is
     *    represented as a `Promise` until the actual state object is needed
     *    (upload|abort).
     * 3. If no `'upload_id'` provided, a new state object is created with just
     *    provided params. It is assumed that the upload_id value will be set
     *    later via the `UploadState::setUploadId()` method.
     */
    private function prepareState()
    {
        // If the state was provided, then just set it.
        if ($this->config['state'] instanceof UploadState) {
            $this->state = $this->config['state'];
            return;
        }

        // Otherwise, construct a new state from the provided identifiers.
        $required = $this->info['id'];
        $uploadIdParam = array_pop($required);
        $id = [$uploadIdParam => $this->config['upload_id']];
        foreach ($required as $key => $param) {
            if (!$this->config[$key]) {
                throw new IAE('You must provide a value for "' . $key . '" in '
                    . 'your config for the MultipartUploader for '
                    . $this->client->getApi()->getServiceFullName() . '.');
            }
            $id[$param] = $this->config[$key];
        }
        $this->state = new UploadState($id);

        if ($this->config['upload_id']) {
            // If uploadId is known, then load the upload data from the service.
            $this->state = $this->loadStateFromService();
        } else {
            // Otherwise, set the partSize based on the config and info.
            $this->state->setPartSize($this->determinePartSize());
        }
    }

    /**
     * Turns the provided source into a stream and stores it.
     *
     * If a string is provided, it is assumed to be a filename, otherwise, it
     * passes the value directly to `Psr7\stream_for()`.
     *
     * @param mixed $source
     */
    private function prepareSource($source)
    {
        // Use the contents of a file as the data source.
        if (is_string($source)) {
            $source = Psr7\try_fopen($source, 'r');
        }

        // Create a source stream.
        $this->source = Psr7\stream_for($source);
        if (!$this->source->isReadable()) {
            throw new IAE('Source stream must be readable.');
        }
    }

    /**
     * Creates a command with all of the relevant parameters from the operation
     * name and an array of additional parameters.
     *
     * @param string $operation      Name of the operation (e.g., UploadPart).
     * @param array  $computedParams Extra params not stored in the Uploader.
     *
     * @return CommandInterface
     */
    private function createCommand($operation, array $computedParams = [])
    {
        $configParams = $this->state->getId() + $this->config[$operation];

        return $this->client->getCommand(
            $this->info['command'][$operation],
            $computedParams + $configParams
        );
    }

    /**
     * Returns a middleware for processing responses of part upload operations.
     *
     * - Adds an onFulfilled callback that calls the service-specific
     *   handleResult method on the Result of the operation.
     * - Adds an onRejected callback that adds the error to an array of errors.
     * - Has a passedByRef $errors arg that the exceptions get added to. The
     *   caller should use that &$errors array to do error handling.
     *
     * @param array $errors Errors from upload operations are added to this.
     *
     * @return callable
     */
    private function getResultHandler(&$errors = [])
    {
        return function (callable $handler) use (&$errors) {
            return function (
                CommandInterface $command,
                RequestInterface $request = null
            ) use ($handler, &$errors) {
                return $handler($command, $request)->then(
                    function (ResultInterface $result) use ($command) {
                        $this->handleResult($command, $result);
                        return $result;
                    },
                    function (AwsException $e) {
                        $errors[$e->getCommand()[$this->info['part']['param']]] = $e;
                    }
                );
            };
        };
    }

    /**
     * Creates a generator that yields part data for the upload's source.
     *
     * Yields associative arrays of parameters that are ultimately merged in
     * with others to form the complete parameters of an UploadPart (or
     * UploadMultipartPart for Glacier) command. This includes the Body
     * parameter, which is a limited stream (i.e., a Stream object, decorated
     * with a LimitStream).
     **
     * @return \Generator
     */
    private function getUploadCommands($resultHandler)
    {
        // Determine if the source can be seeked.
        $seekable = $this->source->isSeekable()
            && $this->source->getMetadata('wrapper_type') === 'plainfile';

        for (
            $partNumber = 1;
            $seekable
                ? $this->source->tell() < $this->source->getSize()
                : !$this->source->eof();
            $partNumber++
        ) {
            // If we haven't already uploaded this part, yield a new part.
            if (!$this->state->hasPartBeenUploaded($partNumber)) {
                $partStartPos = $this->source->tell();
                $command = $this->createCommand('upload', $this->createPart($seekable, $partNumber));
                $command->getHandlerList()->append('sign:mup', $resultHandler);
                yield $command;
                if ($this->source->tell() > $partStartPos) {
                    continue;
                }
            }

            // Advance the source's offset if not already advanced.
            if ($seekable) {
                $this->source->seek(min(
                    $this->source->tell() + $this->state->getPartSize(),
                    $this->source->getSize()
                ));
            } else {
                $this->source->read($this->state->getPartSize());
            }
        }
    }
}
