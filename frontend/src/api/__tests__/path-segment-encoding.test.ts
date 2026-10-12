import { apiFetch } from '@/api/client';
import { getDataset } from '@/api/datasets';
import { fetchColumnValues } from '@/api/maps';

vi.mock('@/api/client', () => ({
  apiFetch: vi.fn(),
  authenticatedRawFetch: vi.fn(),
}));

const mockApiFetch = vi.mocked(apiFetch);

describe('API path segments', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockApiFetch.mockResolvedValue({});
  });

  it('keeps a crafted id inside its own segment', async () => {
    await getDataset('../admin/users?skip=0');

    const path = mockApiFetch.mock.calls[0][0];
    expect(path).toBe('/datasets/..%2Fadmin%2Fusers%3Fskip%3D0');
    const resolved = new URL(`/api${path}`, 'https://geolens.example');
    expect(resolved.pathname).toBe('/api/datasets/..%2Fadmin%2Fusers%3Fskip%3D0');
    expect(resolved.search).toBe('');
  });

  it('encodes each segment once and leaves the query string alone', async () => {
    await fetchColumnValues('ds-1', 'land use', 5);

    expect(mockApiFetch).toHaveBeenCalledWith('/datasets/ds-1/columns/land%20use/values/?limit=5');
  });
});
