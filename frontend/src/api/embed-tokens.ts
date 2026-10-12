import { apiFetch } from '@/api/client';
import type { EmbedTokenCreatedResponse, EmbedTokenResponse } from '@/types/api';

export function createEmbedToken(
  mapId: string,
  expiresInDays?: number,
  allowedOrigins?: string[],
): Promise<EmbedTokenCreatedResponse> {
  return apiFetch<EmbedTokenCreatedResponse>(
    `/maps/${encodeURIComponent(mapId)}/embed-tokens/`,
    {
      method: 'POST',
      body: JSON.stringify({
        expires_in_days: expiresInDays ?? 30,
        allowed_origins: allowedOrigins ?? null,
      }),
    },
  );
}

export async function listEmbedTokens(
  mapId: string,
): Promise<{ tokens: EmbedTokenResponse[]; total: number }> {
  return apiFetch<{ tokens: EmbedTokenResponse[]; total: number }>(
    `/maps/${encodeURIComponent(mapId)}/embed-tokens/`,
  );
}

export async function updateEmbedTokenOrigins(
  mapId: string,
  tokenId: string,
  allowedOrigins: string[] | null,
): Promise<EmbedTokenResponse> {
  return apiFetch<EmbedTokenResponse>(
    `/maps/${encodeURIComponent(mapId)}/embed-tokens/${encodeURIComponent(tokenId)}/`,
    {
      method: 'PATCH',
      body: JSON.stringify({ allowed_origins: allowedOrigins }),
    },
  );
}

export async function revokeEmbedToken(
  mapId: string,
  tokenId: string,
): Promise<EmbedTokenResponse> {
  return apiFetch<EmbedTokenResponse>(
    `/maps/${encodeURIComponent(mapId)}/embed-tokens/${encodeURIComponent(tokenId)}/`,
    { method: 'DELETE' },
  );
}
