import express, { Router } from 'express';
import {
  getUserGroupById,
  listUserGroups,
  createUserGroup,
  updateUserGroup,
  loadUserGroup
} from '../controllers/admin';

export const adminRouter = Router();

const jsonParser = express.json();

adminRouter.get('/group', listUserGroups);
adminRouter.post('/group', jsonParser, createUserGroup);

adminRouter.get('/group/:user_group_id', loadUserGroup, getUserGroupById);
adminRouter.patch('/group/:user_group_id', loadUserGroup, jsonParser, updateUserGroup);
