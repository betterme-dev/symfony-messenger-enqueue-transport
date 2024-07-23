<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Enqueue\MessengerAdapter\Tests;

use Enqueue\MessengerAdapter\AmqpContextManager;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\Impl\AmqpBind;
use Interop\Queue\Context;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;
use Throwable;

class AmqpContextManagerTest extends TestCase
{
    use ProphecyTrait;

    private function getContextManager(
        $topicName,
        $queueName,
        $topicType = null,
        $topicFlags = null,
        $queueBindingKey = null,
        $queueFlags = null,
        $queueArguments = null,
    ): AmqpContextManager {
        $topicProphecy = $this->prophesize(AmqpTopic::class);
        $topicProphecy->setType($topicType ?? AmqpTopic::TYPE_FANOUT)->shouldBeCalled();
        if (null === $topicFlags) {
            $topicProphecy->getFlags()->shouldBeCalled()->willReturn(AmqpTopic::FLAG_PASSIVE);
        }
        $topicProphecy->setFlags($topicFlags ?? (AmqpTopic::FLAG_DURABLE | AmqpTopic::FLAG_PASSIVE))->shouldBeCalled();
        $topic = $topicProphecy->reveal();

        $queueProphecy = $this->prophesize(AmqpQueue::class);
        if (null === $queueFlags) {
            $queueProphecy->getFlags()->shouldBeCalled()->willReturn(AmqpQueue::FLAG_PASSIVE);
        }
        $queueProphecy->setFlags($queueFlags ?? (AmqpQueue::FLAG_DURABLE | AmqpQueue::FLAG_PASSIVE))->shouldBeCalled();
        $queueProphecy->setArguments($queueArguments ?? [])->shouldBeCalled();

        $queue = $queueProphecy->reveal();

        $bind = new AmqpBind($queue, $topic, $queueBindingKey);

        $contextProphecy = $this->prophesize(AmqpContext::class);
        $contextProphecy->createTopic($topicName)->shouldBeCalled()->willReturn($topic);
        $contextProphecy->declareTopic($topic)->shouldBeCalled();
        $contextProphecy->createQueue($queueName)->shouldBeCalled()->willReturn($queue);
        $contextProphecy->declareQueue($queue)->shouldBeCalled();
        $contextProphecy->bind($bind)->shouldBeCalled();
        $context = $contextProphecy->reveal();

        return new AmqpContextManager($context);
    }

    public function testDefaultEnsure(): void
    {
        $topicName = 'foo';
        $queueName = 'bar';
        $contextManager = $this->getContextManager($topicName, $queueName);

        $this->assertTrue($contextManager->ensureExists([
            'topic' => $topicName,
            'topicOptions' => ['name' => $topicName],
            'queue' => $queueName,
            'queueOptions' => ['name' => $queueName],
        ]));
    }

    public function testCustomizedEnsure(): void
    {
        $topicName = 'foo';
        $queueName = 'bar';
        $topicType = 'topic';
        $topicFlags = '8';
        $queueFlags = 16;
        $queueBindingKey = 'foo.#';
        $queueArguments = ['x-max-priority' => 10, 'x-consumer-timeout' => '72000', 'x-my-argument' => 'foo'];
        $contextManager = $this->getContextManager(
            $topicName,
            $queueName,
            $topicType,
            $topicFlags,
            $queueBindingKey,
            $queueFlags,
            $queueArguments,
        );

        $this->assertTrue($contextManager->ensureExists([
            'topic' => $topicName,
            'topicOptions' => ['name' => $topicName, 'type' => $topicType, 'flags' => $topicFlags],
            'queue' => $queueName,
            'queueOptions' => [
                'name' => $queueName,
                'bindingKey' => $queueBindingKey,
                'flags' => $queueFlags,
                'arguments' => $queueArguments,
            ],
        ]));
    }

    public function testInvalidIntegerArgumentTypeEnsure(): void
    {
        $topicName = 'foo';
        $queueName = 'bar';
        $queueArguments = ['x-max-priority' => 10, 'x-consumer-timeout' => 'string', 'x-my-argument' => 'foo'];

        $topicProphecy = $this->prophesize(AmqpTopic::class);
        $topicProphecy->setType(AmqpTopic::TYPE_FANOUT)->shouldBeCalled();
        $topicProphecy->getFlags()->shouldBeCalled()->willReturn(AmqpTopic::FLAG_PASSIVE);
        $topicProphecy->setFlags(AmqpTopic::FLAG_DURABLE | AmqpTopic::FLAG_PASSIVE)->shouldBeCalled();
        $topic = $topicProphecy->reveal();

        $queueProphecy = $this->prophesize(AmqpQueue::class);
        $queueProphecy->getFlags()->shouldBeCalled()->willReturn(AmqpQueue::FLAG_PASSIVE);
        $queueProphecy->setFlags(AmqpQueue::FLAG_DURABLE | AmqpQueue::FLAG_PASSIVE)->shouldBeCalled();
        $queueProphecy->setArguments($queueArguments)->shouldNotBeCalled();

        $queue = $queueProphecy->reveal();

        $contextProphecy = $this->prophesize(AmqpContext::class);
        $contextProphecy->createTopic($topicName)->shouldBeCalled()->willReturn($topic);
        $contextProphecy->declareTopic($topic)->shouldBeCalled();
        $contextProphecy->createQueue($queueName)->shouldBeCalled()->willReturn($queue);
        $contextProphecy->declareQueue($queue)->shouldBeCalled();
        $context = $contextProphecy->reveal();

        $contextManager = new AmqpContextManager($context);

        try {
            $contextManager->ensureExists([
                'topic' => $topicName,
                'topicOptions' => ['name' => $topicName],
                'queue' => $queueName,
                'queueOptions' => [
                    'name' => $queueName,
                    'arguments' => $queueArguments,
                ],
            ]);
        } catch (Throwable $e) {
            $this->assertInstanceOf(InvalidArgumentException::class, $e);
            $this->assertSame(
                'Integer expected for queue argument "x-consumer-timeout", "string" given.',
                $e->getMessage(),
            );
        }
    }

    public function testNotProcessedEnsure(): void
    {
        $topicName = 'foo';
        $queueName = 'bar';

        $contextProphecy = $this->prophesize(Context::class);
        $context = $contextProphecy->reveal();

        $contextManager = new AmqpContextManager($context);

        $this->assertFalse($contextManager->ensureExists([
            'topic' => $topicName,
            'topicOptions' => ['name' => $topicName],
            'queue' => $queueName,
            'queueOptions' => ['name' => $queueName],
        ]));
    }

    public function testContextRetrieving(): void
    {
        $contextProphecy = $this->prophesize(Context::class);
        $context = $contextProphecy->reveal();

        $contextManager = new AmqpContextManager($context);

        $this->assertSame($context, $contextManager->Context());
    }

    /* public function testExceptionRecovering()
    {
        $topicName = 'foo';
        $queueName = 'bar';
        $contextManager = $this->getContextManager($topicName, $queueName);

        $exception = new \AMQPQueueException('', 404); // Class 'AMQPQueueException' not found

        $this->assertTrue($contextManager->recoverException($exception, array(
            'topic' => $topicName,
            'topicOptions' => array('name' => $topicName),
            'queue' => $queueName,
            'queueOptions' => array('name' => $queueName),
        )));
    } */
}
