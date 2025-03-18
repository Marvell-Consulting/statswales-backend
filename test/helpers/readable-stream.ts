import { Readable } from 'node:stream';

export const readableToReadableStream = (readable: Readable): ReadableStream => {
  const stream = new ReadableStream({
    start(controller) {
      readable.on('data', (chunk) => {
        controller.enqueue(chunk);
      });
      readable.on('end', () => {
        controller.close();
      });
    }
  });

  return stream;
};
