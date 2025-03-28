export interface ErrorMessage {
  lang: string;
  message: string;
}

export interface Error {
  field: string;
  message: {
    key: string;
    params?: object;
  };
  user_message: ErrorMessage[];
}
