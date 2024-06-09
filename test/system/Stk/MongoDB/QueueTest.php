<?php

namespace StkSystemTest\MongoDB;

use MongoDB\BSON\UTCDateTime;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use PHPUnit\Framework\TestCase;
use Stk\MongoDB\Queue;

/*
 * mongodb testing user:
    db.createUser({ user: "stk-testing", pwd: "1qay2wsx",
        roles: [ { role: "readWrite", db: "stk-tests" }, {role: "dbAdmin", db: "stk-tests"} ]
    });
 *
 */

class QueueTest extends TestCase
{
    protected Database $database;

    protected Collection $collection;

    protected function setUp(): void
    {
        $mc               = new Client('mongodb://stk-testing:1qay2wsx@127.0.0.1/stk-tests');
        $this->database   = $mc->selectDatabase('stk-tests');
        $this->collection = $this->database->selectCollection('queue');
    }

    public function testAdd()
    {
        $queue = new Queue($this->collection);
        $queue->add('mail.send', ['subject' => 'Welcome', 'body' => 'Hello']);
        $task = $this->collection->findOne();
        $this->assertEquals('mail.send', $task['topic']);
        $this->assertEquals(Queue::STATE_PENDING, $task['state']);
        $this->assertEquals(Queue::DEFAULT_MAXTRIES, $task['maxtries']);
        $this->assertEquals(0, $task['tries']);
    }

    public function testGet()
    {
        $payload = ['subject' => 'Welcome', 'body' => 'Hello', 'headers' => ['foo' => 'bar', 'aaa' => 'bbb']];
        $queue   = new Queue($this->collection);
        $queue->add('mail.send', $payload);
        $task = $queue->get('mail.send');
        $this->assertEquals($payload, $task->payload);
    }

    public function testAck()
    {
        $queue = new Queue($this->collection);
        $queue->add('mail.send', ['subject' => 'Welcome', 'body' => 'Hello']);
        $task = $queue->get('mail.send');
        $queue->ack($task->id);
        $task = $this->collection->findOne();
        $this->assertEquals('mail.send', $task['topic']);
        $this->assertEquals(Queue::STATE_COMPLETED, $task['state']);
    }

    public function testErr()
    {
        $queue = new Queue($this->collection);
        $queue->add('mail.send', ['subject' => 'Welcome', 'body' => 'Hello']);
        $task = $queue->get('mail.send');
        $queue->err($task->id, 'failure');
        $task = $this->collection->findOne();
        $this->assertEquals('mail.send', $task['topic']);
        $this->assertEquals(Queue::STATE_ERROR, $task['state']);
        $this->assertEquals('failure', $task['message']);
    }

    public function testGetNoTasks()
    {
        $queue = new Queue($this->collection);
        $task  = $queue->get('nonexistend');
        $this->assertNull($task);
    }

    public function testCreateIndexes()
    {
        $queue = new Queue($this->collection);
        $queue->createIndexes();
        $indexes = $this->collection->listIndexes();

        $index = null;
        foreach ($indexes as $i => $idx) {
            if ($i == 1) {
                $index = $idx;
                break;
            }
        }

        $this->assertEquals(['topic' => 1, 'state' => 1], $index->getKey());
    }

    public function testSelfcareMaxTries()
    {
        $this->collection->insertOne([
            "topic"    => "mail.send",
            "payload"  => [],
            "meta"     => [
                "created"    => new UTCDateTime(),
                "dispatched" => null,
                "completed"  => null
            ],
            "tries"    => 3,
            "maxtries" => 3,
            "state"    => "pending",
            "message"  => ""
        ]);
        $queue = new Queue($this->collection);
        $queue->selfcare();
        $task = $this->collection->findOne();
        $this->assertEquals(Queue::STATE_ERROR, $task['state']);
    }

    public function testSelfcareOrphaned()
    {
        $this->collection->insertOne([
            "topic"    => "mail.send",
            "payload"  => [],
            "meta"     => [
                "created"    => new UTCDateTime(),
                "dispatched" => new UTCDateTime((time() - Queue::DEFAULT_TIMEOUT - 1) * 1000),
                "completed"  => null
            ],
            "tries"    => 0,
            "maxtries" => 3,
            "state"    => "running",
            "message"  => ""
        ]);
        $queue = new Queue($this->collection);
        $queue->selfcare();
        $task = $this->collection->findOne();
        $this->assertEquals(Queue::STATE_PENDING, $task['state']);
        $this->assertNull($task['meta']['dispatched']);
    }

    public function testSelfcareNoOrphaned()
    {
        $this->collection->insertOne([
            "topic"    => "mail.send",
            "payload"  => [],
            "meta"     => [
                "created"    => new UTCDateTime(),
                "dispatched" => new UTCDateTime((time() - Queue::DEFAULT_TIMEOUT + 1) * 1000),
                "completed"  => null
            ],
            "tries"    => 0,
            "maxtries" => 3,
            "state"    => "running",
            "message"  => ""
        ]);
        $queue = new Queue($this->collection);
        $queue->selfcare();
        $task = $this->collection->findOne();
        $this->assertEquals(Queue::STATE_PENDING, $task['state']);
        $this->assertNull($task['meta']['dispatched']);
    }

    protected function tearDown(): void
    {
        $this->database->drop();
    }
}
