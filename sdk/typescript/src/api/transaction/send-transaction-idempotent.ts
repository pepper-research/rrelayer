import { postApi } from '../axios-wrapper';
import { ApiBaseConfig } from '../types';
import { TransactionSent, TransactionToSend } from './types';
import { RATE_LIMIT_HEADER_NAME } from '../index';

export const sendTransactionIdempotent = async (
  chainId: number,
  transactionToSend: TransactionToSend & { externalId: string },
  rateLimitKey: string | undefined,
  baseConfig: ApiBaseConfig
): Promise<TransactionSent> => {
  try {
    const config: any = {};
    if (rateLimitKey) {
      config.headers = {
        [RATE_LIMIT_HEADER_NAME]: rateLimitKey,
      };
    }

    const response = await postApi<TransactionSent>(
      baseConfig,
      `transactions/relayers/${chainId}/send-idempotent`,
      {
        ...transactionToSend,
      },
      config
    );
    return response.data;
  } catch (error) {
    console.error('Failed to sendTransactionIdempotent', error);
    throw error;
  }
};
