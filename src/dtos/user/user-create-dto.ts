import { IsEmail } from 'class-validator';

export class UserCreateDTO {
  @IsEmail()
  email: string;
}
