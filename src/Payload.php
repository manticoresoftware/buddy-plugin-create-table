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
	    . '(\((?P<structure>.+?)\)\s*)?' // Matches the table structure
	    . '(?P<options>(?:\w+=\d+|\'\d+\')\s*(?:\w+=\d+|\'\d+\')*\s*)' // Matches all options as a single string
	    . '(?P<extra>.*)/ius';

		if (!preg_match($pattern, $request->payload, $matches)) {
			QueryParseError::throw('Failed to parse query');
		}

		// Split the options string into key-value pairs
		if (!preg_match_all(
			'/(?P<key>\w+)=(?P<value>\d+|\'\d+\')/',
			$matches['options'],
			$optionMatches,
			PREG_SET_ORDER
		)) {
			QueryParseError::throw('Failed to options in the query');
		}

		$options = [];
		foreach ($optionMatches as $optionMatch) {
	    $key = $optionMatch['key'];
	    $value = trim($optionMatch['value'], "'");
	    $options[$key] = $value;
		}

		$self = new static();
		// We just need to do something, but actually its' just for PHPstan
		$self->path = $request->path;
		$self->cluster = $matches['cluster'] ?? '';
		$self->table = $matches['table'];
		$self->structure = $matches['structure'];
		$self->shardCount = (int)($options['shards'] ?? 2);
		$self->replicationFactor = (int)($options['rf'] ?? 2);
		$self->extra = $matches['extra'];
		$self->validate();
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
	 * Run query parsed data validation
	 * @return void
	 */
	protected function validate(): void {
		if (!$this->cluster && $this->replicationFactor > 1) {
			throw QueryParseError::create('You cannot set rf greater than 1 when creating single node sharded table.');
		}
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
