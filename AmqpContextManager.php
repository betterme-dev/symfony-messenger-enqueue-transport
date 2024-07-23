<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Enqueue\MessengerAdapter;

use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\Impl\AmqpBind;
use Interop\Queue\Context;

use InvalidArgumentException;

use function array_key_exists;

class AmqpContextManager implements ContextManager
{
    private const ARGUMENTS_AS_INTEGER = [
        'x-delay',
        'x-expires',
        'x-max-length',
        'x-max-length-bytes',
        'x-max-priority',
        'x-message-ttl',
        'x-consumer-timeout',
    ];

    private Context $context;

    public function __construct(Context $context)
    {
        $this->context = $context;
    }

    /**
     * {@inheritdoc}
     */
    public function context(): Context
    {
        return $this->context;
    }

    /**
     * {@inheritdoc}
     */
    public function recoverException(\Exception $exception, array $destination): bool
    {
        if ($exception instanceof \AMQPQueueException) {
            if (404 === $exception->getCode()) {
                return $this->ensureExists($destination);
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function ensureExists(array $destination): bool
    {
        if (!$this->context instanceof AmqpContext) {
            return false;
        }

        $topic = $this->context->createTopic($destination['topic']);
        $topic->setType($destination['topicOptions']['type'] ?? AmqpTopic::TYPE_FANOUT);
        $topicFlags = $destination['topicOptions']['flags'] ?? ((int)$topic->getFlags() | AmqpTopic::FLAG_DURABLE);
        $topic->setFlags($topicFlags);
        $this->context->declareTopic($topic);

        $queue = $this->context->createQueue($destination['queue']);
        $queueFlags = $destination['queueOptions']['flags'] ?? ((int)$queue->getFlags() | AmqpQueue::FLAG_DURABLE);
        $queue->setFlags($queueFlags);
        $this->context->declareQueue($queue);

        $queueArguments = $destination['queueOptions']['arguments'] ?? [];
        $queue->setArguments($this->normalizeQueueArguments($queueArguments));

        $this->context->bind(
            new AmqpBind($queue, $topic, $destination['queueOptions']['bindingKey'] ?? null),
        );

        return true;
    }

    /**
     * Normalizes queue arguments to ensure they are integers.
     *
     * This method iterates over a predefined list of argument keys that are expected to be integers.
     * If an argument is found in the input array but is not numeric, an InvalidArgumentException is thrown.
     * Numeric arguments are cast to integers to ensure type consistency.
     *
     * @param array $arguments Associative array of queue arguments where the key is the argument
     *                         name and the value is the argument value.
     * @return array The modified arguments array with all specified keys having integer values.
     * @throws InvalidArgumentException If an expected integer argument is not numeric.
     */
    private function normalizeQueueArguments(array $arguments): array
    {
        foreach (self::ARGUMENTS_AS_INTEGER as $key) {
            if (!array_key_exists($key, $arguments)) {
                continue;
            }

            if (!is_numeric($arguments[$key])) {
                throw new InvalidArgumentException(
                    sprintf(
                        'Integer expected for queue argument "%s", "%s" given.',
                        $key,
                        get_debug_type($arguments[$key]),
                    ),
                );
            }

            $arguments[$key] = (int)$arguments[$key];
        }

        return $arguments;
    }
}
