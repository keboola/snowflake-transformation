<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->arrayNode('steps')
                    ->prototype('array')
                    ->children()
                        ->scalarNode('name')->end()
                        ->enumNode('execution')
                            ->values(['parallel', 'serial'])
                        ->end()
                        ->arrayNode('blocks')
                            ->prototype('array')
                            ->children()
                                ->scalarNode('name')->end()
                                ->arrayNode('script')
                                    ->prototype('scalar')->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
