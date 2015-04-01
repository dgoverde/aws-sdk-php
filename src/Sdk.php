<?php
namespace Aws;

use Aws\Handler\GuzzleV5\GuzzleHandler;

/**
 * Builds AWS clients based on configuration settings.
 *
 * @method \Aws\AutoScaling\AutoScalingClient createAutoScaling(array $args = [])
 * @method \Aws\CloudFormation\CloudFormationClient createCloudFormation(array $args = [])
 * @method \Aws\CloudFront\CloudFrontClient createCloudFront(array $args = [])
 * @method \Aws\CloudHsm\CloudHsmClient createCloudHsm(array $args = [])
 * @method \Aws\CloudSearch\CloudSearchClient createCloudSearch(array $args = [])
 * @method \Aws\CloudSearchDomain\CloudSearchDomainClient createCloudSearchDomain(array $args = [])
 * @method \Aws\CloudTrail\CloudTrailClient createCloudTrail(array $args = [])
 * @method \Aws\CloudWatch\CloudWatchClient createCloudWatch(array $args = [])
 * @method \Aws\CloudWatchLogs\CloudWatchLogsClient createCloudWatchLogs(array $args = [])
 * @method \Aws\CognitoIdentity\CognitoIdentityClient createCognitoIdentity(array $args = [])
 * @method \Aws\CognitoSync\CognitoSyncClient createCognitoSync(array $args = [])
 * @method \Aws\DataPipeline\DataPipelineClient createDataPipeline(array $args = [])
 * @method \Aws\DirectConnect\DirectConnectClient createDirectConnect(array $args = [])
 * @method \Aws\DynamoDb\DynamoDbClient createDynamoDb(array $args = [])
 * @method \Aws\Ec2\Ec2Client createEc2(array $args = [])
 * @method \Aws\Ecs\EcsClient createEcs(array $args = [])
 * @method \Aws\ElastiCache\ElastiCacheClient createElastiCache(array $args = [])
 * @method \Aws\ElasticBeanstalk\ElasticBeanstalkClient createElasticBeanstalk(array $args = [])
 * @method \Aws\ElasticLoadBalancing\ElasticLoadBalancingClient createElasticLoadBalancing(array $args = [])
 * @method \Aws\ElasticTranscoder\ElasticTranscoderClient createElasticTranscoder(array $args = [])
 * @method \Aws\Emr\EmrClient createEmr(array $args = [])
 * @method \Aws\Glacier\GlacierClient createGlacier(array $args = [])
 * @method \Aws\Iam\IamClient createIam(array $args = [])
 * @method \Aws\ImportExport\ImportExportClient createImportExport(array $args = [])
 * @method \Aws\Kinesis\KinesisClient createKinesis(array $args = [])
 * @method \Aws\OpsWorks\OpsWorksClient createOpsWorks(array $args = [])
 * @method \Aws\Rds\RdsClient createRds(array $args = [])
 * @method \Aws\Redshift\RedshiftClient createRedshift(array $args = [])
 * @method \Aws\Route53\Route53Client createRoute53(array $args = [])
 * @method \Aws\Route53Domains\Route53DomainsClient createRoute53Domains(array $args = [])
 * @method \Aws\S3\S3Client createS3(array $args = [])
 * @method \Aws\Ses\SesClient createSes(array $args = [])
 * @method \Aws\SimpleDb\SimpleDbClient createSimpleDb(array $args = [])
 * @method \Aws\Sns\SnsClient createSns(array $args = [])
 * @method \Aws\Sqs\SqsClient createSqs(array $args = [])
 * @method \Aws\StorageGateway\StorageGatewayClient createStorageGateway(array $args = [])
 * @method \Aws\Sts\StsClient createSts(array $args = [])
 * @method \Aws\Support\SupportClient createSupport(array $args = [])
 * @method \Aws\Swf\SwfClient createSwf(array $args = [])
 */
class Sdk
{
    const VERSION = '3.0.0-beta.1';

    /** @var array Map of custom lowercase names to service endpoint names. */
    private static $aliases = [
        'cloudwatch'      => 'monitoring',
        'cloudwatchlogs'  => 'logs',
        'cognitoidentity' => 'cognito-identity',
        'cognitosync'     => 'cognito-sync',
        'emr'             => 'elasticmapreduce',
        'simpledb'        => 'sdb',
        'ses'             => 'email',
    ];

    /** @var array Arguments for creating clients */
    private $args;

    /**
     * Constructs a new SDK object with an associative array of default
     * client settings.
     *
     * @param array $args
     *
     * @throws \InvalidArgumentException
     * @see Aws\Sdk::getClient for a list of available options.
     */
    public function __construct(array $args = [])
    {
        $this->args = $args;

        if (!isset($args['handler']) && !isset($args['http_handler'])) {
            $this->args['http_handler'] = default_http_handler();
        }
    }

    public function __call($name, array $args = [])
    {
        if (strpos($name, 'create') === 0) {
            return $this->createClient(
                substr($name, 6),
                isset($args[0]) ? $args[0] : []
            );
        }

        throw new \BadMethodCallException("Unknown method: {$name}.");
    }

    /**
     * Create an endpoint prefix name from a namespace.
     *
     * @param string $name Namespace name
     *
     * @return string
     */
    public static function getEndpointPrefix($name)
    {
        $name = strtolower($name);

        return isset(self::$aliases[$name]) ? self::$aliases[$name] : $name;
    }

    /**
     * Get a client by name using an array of constructor options.
     *
     * @param string $name Client namespace name (e.g., DynamoDb).
     * @param array  $args Custom arguments to provide to the client.
     *
     * @return AwsClientInterface
     * @throws \InvalidArgumentException
     * @see Aws\AwsClient::__construct for a list of available options.
     */
    public function createClient($name, array $args = [])
    {
        // Merge provided args with stored args
        if (isset($this->args[$name])) {
            $args += $this->args[$name];
        }

        $args += $this->args;

        if (!isset($args['service'])) {
            $args['service'] = self::getEndpointPrefix($name);
        }

        $client = "Aws\\{$name}\\{$name}Client";

        if (!class_exists($client)) {
            $client = 'Aws\\AwsClient';
        }

        return new $client($args);
    }
}
