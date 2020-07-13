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
                ->integerNode('query_timeout')
                    ->defaultValue(7200)
                ->end()
                ->arrayNode('blocks')
                    ->isRequired()
                    ->prototype('array')
                    ->children()
                        ->scalarNode('name')
                            ->isRequired()
                        ->end()
                        ->arrayNode('codes')
                            ->isRequired()
                            ->prototype('array')
                            ->children()
                                ->scalarNode('name')
                                    ->isRequired()
                                ->end()
                                ->arrayNode('script')
                                    ->isRequired()
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
