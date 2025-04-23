import express, { Router } from 'express';

import {
  listRoles,
  listUserGroups,
  getAllUserGroups,
  loadUserGroup,
  createUserGroup,
  getUserGroupById,
  updateUserGroup,
  listUsers,
  loadUser,
  getUserById,
  createUser,
  updateUserRoles,
  updateUserStatus
} from '../controllers/admin';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { logger } from '../utils/logger';
import { GlobalRole } from '../enums/global-role';

export const adminRouter = Router();

const jsonParser = express.json();

adminRouter.use((req, res, next) => {
  logger.debug(`checking if user is a service admin...`);
  if (!req.user?.globalRoles?.includes(GlobalRole.ServiceAdmin)) {
    next(new ForbiddenException('user is not a service admin'));
    return;
  }
  logger.info(`user is a service admin`);
  next();
});

adminRouter.get('/role', listRoles);

adminRouter.get('/group', getAllUserGroups);
adminRouter.post('/group', jsonParser, createUserGroup);
adminRouter.get('/group/list', listUserGroups);

adminRouter.get('/group/:user_group_id', loadUserGroup, getUserGroupById);
adminRouter.patch('/group/:user_group_id', loadUserGroup, jsonParser, updateUserGroup);

adminRouter.get('/user', listUsers);
adminRouter.post('/user', jsonParser, createUser);

adminRouter.get('/user/:user_id', loadUser, getUserById);
adminRouter.patch('/user/:user_id/role', loadUser, jsonParser, updateUserRoles);

adminRouter.patch('/user/:user_id/status', loadUser, jsonParser, updateUserStatus);
