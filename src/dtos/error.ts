export interface ErrorMessage {
  lang: string;
  message: string;
}

export interface Error {
  field: string;
  message: ErrorMessage[];
  tag: {
    name: string;
    params: object;
  };
}
