import { bffFetch } from './client';

export interface CorsOrigin {
  id: string;
  origin: string;
  description: string | null;
  createdBy: string;
  createdAt: string;
}

export interface CorsOriginListResponse {
  items: CorsOrigin[];
  total: number;
}

export interface CreateCorsOriginRequest {
  origin: string;
  description?: string;
}

export const corsApi = {
  list(): Promise<CorsOriginListResponse> {
    return bffFetch('/admin/platform/cors');
  },

  get(id: string): Promise<CorsOrigin> {
    return bffFetch(`/admin/platform/cors/${id}`);
  },

  getAllowed(): Promise<{ origins: string[] }> {
    return bffFetch('/admin/platform/cors/allowed');
  },

  create(data: CreateCorsOriginRequest): Promise<CorsOrigin> {
    return bffFetch('/admin/platform/cors', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  delete(id: string): Promise<void> {
    return bffFetch(`/admin/platform/cors/${id}`, {
      method: 'DELETE',
    });
  },
};
