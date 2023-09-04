<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\CreateTable;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client;
use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithClient;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;
use parallel\Runtime;

final class Handler extends BaseHandlerWithClient {
	/**
	 * Initialize the executor
	 *
	 * @param Payload $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

  /**
	 * Process the request
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(Runtime $runtime): Task {
		$args = $this->payload->toArgs();

		// We are blocking until final state and return the results
		$taskFn = static function (Payload $payload, Client $client): TaskResult {
			$ts = time();
			$value = [];
			while (true) {
				$q = "select `value` from sharding_state where `key` = 'table:{$payload->table}'";
				$resp = $client->sendRequest($q);
				$result = $resp->getResult();
				/** @var array{0:array{data?:array{0:array{value:string}}}} $result */
				if (isset($result[0]['data'][0]['value'])) {
					$value = json_decode($result[0]['data'][0]['value'], true);
				}
				/** @var array{result:string,status?:string} $value */
				$status = $value['status'] ?? 'processing';
				if ($status !== 'processing') {
					return TaskResult::raw($value['result']);
				}
				if ((time() - $ts) > 15) {
					break;
				}
				usleep(500000);
			}

			return TaskResult::withError('Waiting timeout exceeded.');
		};

		return Task::createInRuntime(
			$runtime, $taskFn, [$this->payload, $this->manticoreClient]
		)->on('run', fn() => static::processHook('shard', [$args]))
		 ->run();
	}
}
