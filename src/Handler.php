<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\CreateTable;

use Manticoresearch\Buddy\Core\Plugin\BaseHandler;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;
use parallel\Runtime;

final class Handler extends BaseHandler {
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
		// TODO: what we should do here?
		$taskFn = static function (): TaskResult {
			return TaskResult::none();
		};

		return Task::createInRuntime(
			$runtime, $taskFn, []
		)->on('run', fn() => static::processHook('shard', [$args]))
		 ->run();
	}

	/**
	 * @return array<string>
	 */
	public function getProps(): array {
		return [];
	}
}
