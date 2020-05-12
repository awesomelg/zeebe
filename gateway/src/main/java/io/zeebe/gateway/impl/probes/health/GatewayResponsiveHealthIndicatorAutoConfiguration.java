/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.probes.health;

import io.zeebe.gateway.impl.configuration.GatewayCfg;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for {@link GatewayResponsiveHealthIndicator}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnEnabledHealthIndicator("gateway-responsive")
@AutoConfigureBefore(HealthContributorAutoConfiguration.class)
@EnableConfigurationProperties(GatewayResponsiveHealthIndicatorProperties.class)
public class GatewayResponsiveHealthIndicatorAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(name = "gateway-responsiveHealthIndicator")
  public GatewayResponsiveHealthIndicator gatewayResponsiveHealthIndicator(
      GatewayCfg gatewayCfg, GatewayResponsiveHealthIndicatorProperties properties) {
    return new GatewayResponsiveHealthIndicator(gatewayCfg, properties.getRequestTimeout());
  }
}