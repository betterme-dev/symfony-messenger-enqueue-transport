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

use Interop\Queue\Context;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use RuntimeException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Symfony Messenger transport factory.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
readonly class QueueInteropTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private SerializerInterface $serializer,
        private ContainerInterface $container,
        private bool $debug = false,
    ) {
    }

    // BC layer for Symfony 4.1 beta1
    public function createReceiver(string $dsn, array $options): TransportInterface
    {
        return $this->createTransport($dsn, $options);
    }

    // BC layer for Symfony 4.1 beta1
    public function createSender(string $dsn, array $options): TransportInterface
    {
        return $this->createTransport($dsn, $options);
    }

    public function createTransport(
        string $dsn,
        array $options,
        SerializerInterface $serializer = null,
    ): TransportInterface {
        [$contextManager, $dsnOptions] = $this->parseDsn($dsn);

        $options = array_merge($dsnOptions, $options);

        return new QueueInteropTransport(
            $serializer ?? $this->serializer,
            $contextManager,
            $options,
            $this->debug,
        );
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'enqueue://');
    }

    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    private function parseDsn(string $dsn): array
    {
        $parsedDsn = parse_url($dsn);
        $enqueueContextName = $parsedDsn['host'];

        $amqpOptions = [];
        if (isset($parsedDsn['query'])) {
            parse_str($parsedDsn['query'], $parsedQuery);
            $parsedQuery = array_map(function ($e) {
                return is_numeric($e) ? (int)$e : $e;
            }, $parsedQuery);
            $amqpOptions = array_replace_recursive($amqpOptions, $parsedQuery);
        }

        if (!$this->container->has($contextService = 'enqueue.transport.' . $enqueueContextName . '.context')) {
            throw new RuntimeException(
                sprintf(
                    'Can\'t find Enqueue\'s transport named "%s": Service "%s" is not found.',
                    $enqueueContextName,
                    $contextService,
                ),
            );
        }

        $context = $this->container->get($contextService);
        if (!$context instanceof Context) {
            throw new RuntimeException(sprintf('Service "%s" not instanceof context', $contextService));
        }

        return [
            new AmqpContextManager($context),
            $amqpOptions,
        ];
    }
}
