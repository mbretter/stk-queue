<?php

namespace Stk\MongoDB;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Operation\FindOneAndUpdate;
use stdClass;
use Stk\Service\Injectable;
use MongoDB\Collection;

class Queue implements Injectable
{
    protected Collection $collection;

    public const DEFAULT_MAXTRIES = 3;
    public const DEFAULT_TIMEOUT  = 300; // 300secs def timeout for running tasks

    public const STATE_PENDING   = 'pending';
    public const STATE_RUNNING   = 'running';
    public const STATE_COMPLETED = 'completed';
    public const STATE_ERROR     = 'error';

    public function __construct(Collection $collection)
    {
        $this->collection = $collection;
    }

    public function add(string $topic, array $payload = [], int $maxTries = self::DEFAULT_MAXTRIES): void
    {
        $this->collection->insertOne([
            'topic'    => $topic,
            'payload'  => $payload,
            'meta'     => [
                'created'    => new UTCDatetime(),
                'dispatched' => null,
                'completed'  => null
            ],
            'tries'    => 0,
            'maxtries' => $maxTries,
            'state'    => self::STATE_PENDING,
            'message'  => ''
        ]);
    }

    public function get(string $topic): ?stdClass
    {
        /** @var stdClass $task */
        $task = $this->collection->findOneAndUpdate([
            'topic' => $topic,
            'state' => self::STATE_PENDING,
            '$expr' => ['$lt' => ['$tries', '$maxtries']]
        ], [
            '$set' => [
                'state'           => self::STATE_RUNNING,
                'meta.dispatched' => new UTCDatetime()
            ],
            '$inc' => [
                'tries' => 1
            ]
        ], [
            'returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER,
            'typeMap'        => [
                'root'       => 'object',
                'document'   => 'object',
                'array'      => 'array',
                'fieldPaths' => [
                    'payload'   => 'array',
                    'payload.$' => 'array',
                ],
            ]
        ]);

        if ($task !== null) {
            $task->id = (string) $task->_id;
            unset($task->_id);
            $task->meta->created = $task->meta->created->toDateTime();
            if ($task->meta->dispatched instanceof UTCDateTime) {
                $task->meta->dispatched = $task->meta->dispatched->toDateTime();
            }
            if ($task->meta->completed instanceof UTCDateTime) {
                $task->meta->completed = $task->meta->completed->toDateTime();
            }
        }

        return $task;
    }

    public function ack(string $id): void
    {
        $this->collection->updateOne([
            '_id' => new ObjectId($id),
        ], [
            '$set' => [
                'state'          => self::STATE_COMPLETED,
                'meta.completed' => new UTCDatetime()
            ]
        ]);
    }

    public function err(string $id, string $message = ''): void
    {
        $this->collection->updateOne([
            '_id' => new ObjectId($id),
        ], [
            '$set' => [
                'state'          => self::STATE_ERROR,
                'meta.completed' => new UTCDatetime(),
                'message'        => $message
            ]
        ]);
    }

    public function selfcare(string $topic = null): void
    {
        // re-schedule long running tasks
        // this only happens if the processor could not ack the task, i.e. the application crashed
        $query = [
            'state'           => 'running',
            'meta.dispatched' => ['$lt' => new UTCDateTime((time() + self::DEFAULT_TIMEOUT) * 1000)]
        ];

        if ($topic !== null) {
            $query['topic'] = $topic;
        }

        $this->collection->updateMany($query, [
            '$set' => [
                'state'           => self::STATE_PENDING,
                'meta.dispatched' => null,
            ]
        ]);

        // set tasks exceeding maxtries to error
        $query = [
            'state' => self::STATE_PENDING,
            '$expr' => ['$gte' => ['$tries', '$maxtries']]
        ];

        if ($topic !== null) {
            $query['topic'] = $topic;
        }

        $this->collection->updateMany($query, [
            '$set' => [
                'state' => self::STATE_ERROR,
            ]
        ]);
    }

    public function createIndexes(): void
    {
        $this->collection->createIndex([
            'topic' => 1,
            'state' => 1
        ]);
    }
}
