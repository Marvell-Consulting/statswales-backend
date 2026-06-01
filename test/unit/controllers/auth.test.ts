import { Request, Response } from 'express';
import passport from 'passport';
import jwt from 'jsonwebtoken';

import { checkTokenFitsInCookie, loginEntraID } from '../../../src/controllers/auth';
import { config } from '../../../src/config';

describe('auth controller', () => {
  describe('checkTokenFitsInCookie', () => {
    it('does not throw for a token within the 4096 byte limit', () => {
      expect(() => checkTokenFitsInCookie('a'.repeat(4096))).not.toThrow();
    });

    it('throws for a token larger than 4096 bytes', () => {
      expect(() => checkTokenFitsInCookie('a'.repeat(4097))).toThrow(/exceeds the maximum cookie size/);
    });
  });

  describe('loginEntraID', () => {
    const returnURL = `${config.frontend.url}/auth/callback`;

    let req: { login: jest.Mock };
    let res: Partial<Response> & { redirect: jest.Mock; cookie: jest.Mock };
    let next: jest.Mock;

    // Replace passport.authenticate with a stub that immediately invokes the controller's
    // callback with the supplied (err, user, info), mimicking a finished EntraID round-trip.
    const stubAuthenticate = (err: Error | null, user: unknown, info?: Record<string, string>) => {
      jest
        .spyOn(passport, 'authenticate')
        .mockImplementation(
          ((_strategy: unknown, _opts: unknown, cb: (e: Error | null, u: unknown, i?: unknown) => void) => () =>
            cb(err, user, info)) as unknown as typeof passport.authenticate
        );
    };

    beforeEach(() => {
      req = { login: jest.fn((_user, _opts, cb: (e: Error | null) => void) => cb(null)) };
      res = { redirect: jest.fn(), cookie: jest.fn() };
      next = jest.fn();
    });

    afterEach(() => jest.restoreAllMocks());

    it('redirects with error=provider when passport returns an error', () => {
      stubAuthenticate(new Error('boom'), undefined);
      loginEntraID(req as unknown as Request, res as Response, next);
      expect(res.redirect).toHaveBeenCalledWith(`${returnURL}?error=provider`);
      expect(res.cookie).not.toHaveBeenCalled();
    });

    it('redirects with error=provider when no user is returned', () => {
      stubAuthenticate(null, undefined, { message: 'no user' });
      loginEntraID(req as unknown as Request, res as Response, next);
      expect(res.redirect).toHaveBeenCalledWith(`${returnURL}?error=provider`);
    });

    it('redirects with error=login when req.login fails', () => {
      stubAuthenticate(null, { id: 'user-1', email: 'a@b.com' });
      req.login = jest.fn((_user, _opts, cb: (e: Error | null) => void) => cb(new Error('login failed')));
      loginEntraID(req as unknown as Request, res as Response, next);
      expect(res.redirect).toHaveBeenCalledWith(`${returnURL}?error=login`);
      expect(res.cookie).not.toHaveBeenCalled();
    });

    it('sets a jwt cookie and redirects to the callback on success', () => {
      const user = { id: 'user-1', email: 'a@b.com', name: 'A B', status: 'active', globalRoles: [], groupRoles: [] };
      stubAuthenticate(null, user);

      loginEntraID(req as unknown as Request, res as Response, next);

      expect(res.cookie).toHaveBeenCalledWith('jwt', expect.any(String), expect.objectContaining({ httpOnly: true }));
      expect(res.redirect).toHaveBeenCalledWith(returnURL);

      const token = res.cookie.mock.calls[0][1] as string;
      const decoded = jwt.verify(token, config.auth.jwt.secret) as { user: { id: string } };
      expect(decoded.user.id).toBe('user-1');
    });
  });
});
