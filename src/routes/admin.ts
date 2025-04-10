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

export const adminRouter = Router();

const jsonParser = express.json();

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
