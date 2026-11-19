package no.fdk.harvester.config

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.filter.OncePerRequestFilter
import java.security.MessageDigest

class ApiKeyAuthenticationFilter(private val apiKey: String) : OncePerRequestFilter() {
    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, filterChain: FilterChain) {
        val requestApiKey = request.getHeader(API_KEY_HEADER)
        if (requestApiKey != null && MessageDigest.isEqual(requestApiKey.toByteArray(), apiKey.toByteArray())) {
            SecurityContextHolder.getContext().authentication =
                UsernamePasswordAuthenticationToken("api-user", null, listOf(SimpleGrantedAuthority("ROLE_API_USER")))
        }
        filterChain.doFilter(request, response)
    }

    companion object {
        const val API_KEY_HEADER = "X-API-KEY"
    }
}
