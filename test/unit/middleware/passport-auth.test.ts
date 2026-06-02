import * as openIdClient from 'openid-client';

import { entraIdVerify } from '../../../src/middleware/passport-auth';
import { UserRepository } from '../../../src/repositories/user';
import { AuthProvider } from '../../../src/enums/auth-providers';

// Avoid loading the real ESM openid-client / its passport strategy. We only exercise the verify
// callback, which needs fetchUserInfo; the strategy class is never instantiated in these tests.
jest.mock('openid-client', () => ({
  discovery: jest.fn(),
  fetchUserInfo: jest.fn()
}));

jest.mock('openid-client/passport', () => ({
  Strategy: class {}
}));

jest.mock('../../../src/repositories/user', () => ({
  UserRepository: {
    findOne: jest.fn(),
    save: jest.fn(),
    merge: jest.fn((entity, changes) => Object.assign(entity, changes))
  }
}));

const fetchUserInfo = openIdClient.fetchUserInfo as jest.Mock;
const findOne = UserRepository.findOne as jest.Mock;
const save = UserRepository.save as jest.Mock;

// minimal Tokens stand-in: verify only reads claims().sub and access_token
const tokens = { claims: () => ({ sub: 'entra-sub-123' }), access_token: 'access-token' } as never;

const verify = entraIdVerify({} as never);

describe('passport-auth entraIdVerify', () => {
  beforeEach(() => jest.clearAllMocks());

  it('rejects when the entraid account is missing an email', async () => {
    fetchUserInfo.mockResolvedValue({ sub: 'entra-sub-123' }); // no email
    const done = jest.fn();

    await Promise.resolve(verify(tokens, done));

    expect(done).toHaveBeenCalledWith(null, undefined, {
      message: 'entraid account does not have a user id or email, cannot login'
    });
    expect(save).not.toHaveBeenCalled();
  });

  it('updates and returns a user matched by provider id', async () => {
    fetchUserInfo.mockResolvedValue({ sub: 'entra-sub-123', email: 'Found@Example.com', name: 'Found User' });
    const existing = { id: 'user-1', email: 'old@example.com', provider: AuthProvider.EntraId };
    findOne.mockResolvedValueOnce(existing);
    const done = jest.fn();

    await Promise.resolve(verify(tokens, done));

    expect(findOne).toHaveBeenCalledTimes(1);
    expect(save).toHaveBeenCalledWith(expect.objectContaining({ id: 'user-1', email: 'found@example.com' }));
    expect(done).toHaveBeenCalledWith(null, existing);
  });

  it('associates and returns a user matched by email when no provider id match', async () => {
    fetchUserInfo.mockResolvedValue({ sub: 'entra-sub-123', email: 'match@example.com', name: 'Email User' });
    const existing = { id: 'user-2', email: 'match@example.com', provider: 'local' };
    findOne.mockResolvedValueOnce(null).mockResolvedValueOnce(existing);
    const done = jest.fn();

    await Promise.resolve(verify(tokens, done));

    expect(findOne).toHaveBeenCalledTimes(2);
    expect(save).toHaveBeenCalledWith(
      expect.objectContaining({ id: 'user-2', provider: AuthProvider.EntraId, providerUserId: 'entra-sub-123' })
    );
    expect(done).toHaveBeenCalledWith(null, existing);
  });

  it('rejects when no user matches by provider id or email', async () => {
    fetchUserInfo.mockResolvedValue({ sub: 'entra-sub-123', email: 'nobody@example.com' });
    findOne.mockResolvedValue(null);
    const done = jest.fn();

    await Promise.resolve(verify(tokens, done));

    expect(done).toHaveBeenCalledWith(null, undefined, { message: 'User not recognised' });
    expect(save).not.toHaveBeenCalled();
  });

  it('rejects with a generic message when the lookup throws', async () => {
    fetchUserInfo.mockResolvedValue({ sub: 'entra-sub-123', email: 'boom@example.com' });
    findOne.mockRejectedValue(new Error('db down'));
    const done = jest.fn();

    await Promise.resolve(verify(tokens, done));

    expect(done).toHaveBeenCalledWith(null, undefined, { message: 'Unknown error' });
  });

  it('rejects with a generic message when fetching user info throws', async () => {
    fetchUserInfo.mockRejectedValue(new Error('provider unreachable'));
    const done = jest.fn();

    await Promise.resolve(verify(tokens, done));

    expect(done).toHaveBeenCalledWith(null, undefined, { message: 'Unknown error' });
    expect(findOne).not.toHaveBeenCalled();
  });
});
