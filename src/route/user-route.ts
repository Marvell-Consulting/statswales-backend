import { Router } from 'express';

// eslint-disable-next-line import/no-cycle
import { logger } from '../app';
import { User } from '../entity/user';
import { userToUserDTO } from '../dtos/user-dto';

export const userRoute = Router();

userRoute.get('/:id', async (req, res) => {
    if (!req.params.id) {
        logger.error(`Reequest parameter id is missing`);
        res.status(400).send();
        return;
    }
    const userID = req.params.id;
    const user = await User.findOneBy({ id: userID });
    if (!user) {
        logger.error(`User with id ${req.params.id} not found`);
        res.status(404).send();
        return;
    }
    const userDto = userToUserDTO(user);
    res.json(userDto);
});

userRoute.post('/', async (req, res) => {
    const user = new User();
    user.oidcId = req.body.oidcId;
    user.provider = req.body.provider;
    user.name = req.body.name;
    user.email = req.body.email;
    user.profile = req.body.profile;
    let usr = await User.findOneBy({ oidcId: user.oidcId });
    if (usr === null || usr === undefined) {
        usr = await user.save();
    }
    const userDto = userToUserDTO(usr);
    res.status(201).send();
    res.json(userDto);
});
