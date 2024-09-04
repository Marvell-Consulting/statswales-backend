import { Router } from 'express';
import passport from 'passport';

export const test = Router();

test.get('/no-auth', (req, res) => {
    res.json({ message: 'success' });
});

test.get('/jwt-auth', passport.authenticate('jwt', { session: false }), (req, res) => {
    res.json({ message: 'success', user: req.user });
});
