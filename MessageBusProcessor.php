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

use Enqueue\MessengerAdapter\Exception\RejectMessageException;
use Enqueue\MessengerAdapter\Exception\RequeueMessageException;
use Interop\Queue\Context;
use Interop\Queue\Message;
use Interop\Queue\Processor;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Throwable;

/**
 * The processor could be used with any queue interop compatible consumer, for example Enqueue's QueueConsumer.
 *
 * @author Max Kotliar <kotlyar.maksim@gmail.com>
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
readonly class MessageBusProcessor implements Processor
{

    public function __construct(
        private MessageBusInterface $bus,
        private SerializerInterface $messageDecoder,
    ) {
    }

    /**
     * {@inheritDoc}
     */
    public function process(Message $message, Context $context)
    {
        try {
            $busMessage = $this->messageDecoder->decode([
                'body' => $message->getBody(),
                'headers' => $message->getHeaders(),
                'properties' => $message->getProperties(),
            ]);
        } catch (MessageDecodingFailedException) {
            return Processor::REJECT;
        }

        try {
            $this->bus->dispatch($busMessage);

            return Processor::ACK;
        } catch (RejectMessageException) {
            return Processor::REJECT;
        } catch (RequeueMessageException) {
            return Processor::REQUEUE;
        } catch (Throwable) {
            return Processor::REJECT;
        }
    }
}
