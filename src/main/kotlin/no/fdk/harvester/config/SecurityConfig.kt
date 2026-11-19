package no.fdk.harvester.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.HttpStatusEntryPoint
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter

@Configuration
@EnableWebSecurity
open class SecurityConfig(@Value("\${app.security.api-key:}") private val apiKey: String) {
    @Bean
    open fun securityFilterChain(http: HttpSecurity): SecurityFilterChain {
        if (apiKey.isNotBlank()) {
            http.addFilterBefore(ApiKeyAuthenticationFilter(apiKey), UsernamePasswordAuthenticationFilter::class.java)
        }

        http
            .csrf { it.ignoringRequestMatchers("/actuator/**", "/sources/**") }
            .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
            .exceptionHandling { it.authenticationEntryPoint(HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)) }
            .authorizeHttpRequests { authz ->
                authz
                    .requestMatchers("/actuator/**")
                    .permitAll()
                    .anyRequest()
                    .authenticated()
            }

        return http.build()
    }
}
