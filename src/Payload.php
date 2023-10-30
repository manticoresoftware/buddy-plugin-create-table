<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\CreateTable;

use Manticoresearch\Buddy\Core\Error\QueryParseError;
use Manticoresearch\Buddy\Core\Network\Request;
use Manticoresearch\Buddy\Core\Plugin\BasePayload;

/**
 * This is simple do nothing request that handle empty queries
 * which can be as a result of only comments in it that we strip
 */
final class Payload extends BasePayload {
	public string $path;
	public string $cluster;
	public string $table;
	public string $structure;
	public string $extra;
	public int $shardCount;
	public int $replicationFactor;

  /**
	 * @param Request $request
	 * @return static
	 */
	public static function fromRequest(Request $request): static {
		$pattern = '/CREATE\s+TABLE\s+'
		. '(?:(?P<cluster>[^:\s]+):)?(?P<table>[^:\s\()]+)\s*'
		. '(\((?P<structure>.+?)\)\s*)?' // This line is changed to match table structure
		. '(?:shards=(?P<shards>\d+|\'\d+\')\s+)'
		. '(?:rf=(?P<rf>\d+|\'\d+\')\s*)'
		. '(?P<extra>.*)/ius';

		if (!preg_match($pattern, $request->payload, $matches)) {
			QueryParseError::throw('Failed to parse query');
		}
		$self = new static();
		// We just need to do something, but actually its' just for PHPstan
		$self->path = $request->path;
		$self->cluster = $matches['cluster'] ?? '';
		$self->table = $matches['table'];
		$self->structure = $matches['structure'];
		$self->shardCount = (int)($matches['shards'] ?? 2);
		$self->replicationFactor = (int)($matches['rf'] ?? 2);
		$self->extra = $matches['extra'];
		return $self;
	}

	/**
	 * @param Request $request
	 * @return bool
	 */
	public static function hasMatch(Request $request): bool {
		return strpos($request->error, 'P03') === 0
			&& stripos($request->error, 'syntax error')
			&& stripos($request->payload, 'create table') === 0;
	}

	/**
	 * Convert the current state into array
	 * that we use for args in event
	 * @return array{
	 * table:array{cluster:string,name:string,structure:string,extra:string},
	 * replicationFactor:int,
	 * shardCount:int
	 * }
	 */
	public function toArgs(): array {
		return [
			'table' => [
				'cluster' => $this->cluster,
				'name' => $this->table,
				'structure' => $this->structure,
				'extra' => $this->extra,
			],
			'replicationFactor' => $this->replicationFactor,
			'shardCount' => $this->shardCount,
		];
	}
}
