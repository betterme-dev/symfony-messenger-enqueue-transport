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

use Enqueue\AmqpTools\RabbitMqDelayPluginDelayStrategy;
use Enqueue\MessengerAdapter\AmqpContextManager;
use Enqueue\MessengerAdapter\QueueInteropTransport;
use Enqueue\MessengerAdapter\QueueInteropTransportFactory;
use Interop\Queue\Context;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class QueueInteropTransportFactoryTest extends TestCase
{
    use ProphecyTrait;

    public function testSupports(): void
    {
        $factory = $this->getFactory();

        $this->assertTrue($factory->supports('enqueue://something', []));
        $this->assertFalse($factory->supports('amqp://something', []));
    }

    public function testCreatesTransport(): void
    {
        $serializer = $this->prophesize(SerializerInterface::class);
        $queueContext = $this->prophesize(Context::class)->reveal();

        $container = $this->prophesize(ContainerInterface::class);
        $container->has('enqueue.transport.default.context')->willReturn(true);
        $container->get('enqueue.transport.default.context')->willReturn($queueContext);

        $factory = $this->getFactory($serializer->reveal(), $container->reveal());
        $dsn = 'enqueue://default';

        $expectedTransport = new QueueInteropTransport(
            $serializer->reveal(),
            new AmqpContextManager($queueContext),
            [],
            true,
        );
        $this->assertEquals($expectedTransport, $factory->createTransport($dsn, []));

        // Ensure BC for Symfony beta 4.1
        $this->assertEquals($expectedTransport, $factory->createSender($dsn, []));
        $this->assertEquals($expectedTransport, $factory->createReceiver($dsn, []));
    }

    public function testDnsParsing(): void
    {
        $queueContext = $this->prophesize(Context::class)->reveal();
        $serializer = $this->prophesize(SerializerInterface::class);

        $container = $this->prophesize(ContainerInterface::class);
        $container->has('enqueue.transport.default.context')->willReturn(true);
        $container->get('enqueue.transport.default.context')->willReturn($queueContext);

        $factory = $this->getFactory($serializer->reveal(), $container->reveal());
        $dsn = 'enqueue://default?queue[name]=test&topic[name]=test&deliveryDelay=100&delayStrategy=Enqueue\AmqpTools\RabbitMqDelayPluginDelayStrategy&timeToLive=100&receiveTimeout=100&priority=100';

        $expectedTransport = new QueueInteropTransport(
            $serializer->reveal(),
            new AmqpContextManager($queueContext),
            [
                'topic' => ['name' => 'test'],
                'queue' => ['name' => 'test'],
                'deliveryDelay' => 100,
                'delayStrategy' => RabbitMqDelayPluginDelayStrategy::class,
                'priority' => 100,
                'timeToLive' => 100,
                'receiveTimeout' => 100,
            ],
            true,
        );

        $this->assertEquals($expectedTransport, $factory->createTransport($dsn, []));

        // Ensure BC for Symfony beta 4.1
        $this->assertEquals($expectedTransport, $factory->createSender($dsn, []));
        $this->assertEquals($expectedTransport, $factory->createReceiver($dsn, []));
    }

    public function testItThrowsAnExceptionWhenContextDoesNotExist()
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage(
            "Can't find Enqueue's transport named \"foo\": Service \"enqueue.transport.foo.context\" is not found.",
        );
        $container = $this->prophesize(ContainerInterface::class);
        $container->has('enqueue.transport.foo.context')->willReturn(false);

        $factory = $this->getFactory(container: $container->reveal());
        $factory->createTransport('enqueue://foo', []);
    }

    private function getFactory(
        SerializerInterface $serializer = null,
        ContainerInterface $container = null,
        $debug = true,
    ) {
        return new QueueInteropTransportFactory(
            $serializer ?: $this->prophesize(SerializerInterface::class)->reveal(),
            $container ?: $this->prophesize(ContainerInterface::class)->reveal(),
            $debug,
        );
    }
}
