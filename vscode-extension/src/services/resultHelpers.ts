import { ResultSet } from './sqlGatewayClient';

export function getResultRows(result: ResultSet): any[] {
  return result.results?.data || result.data || [];
}

export function getNextResultToken(nextResultUri: string | undefined, currentToken: number): number {
  if (nextResultUri && nextResultUri.includes('/')) {
    const parsed = parseInt(nextResultUri.split('/').pop() || '', 10);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }

  return currentToken + 1;
}

export function shouldRetryInitialResult(result: ResultSet, currentToken: number = 0): boolean {
  const rawData = getResultRows(result);

  if (result.resultType === 'NOT_READY') {
    return true;
  }

  if (result.resultType !== 'PAYLOAD' || rawData.length > 0) {
    return false;
  }

  if (!result.nextResultUri) {
    return false;
  }

  return getNextResultToken(result.nextResultUri, currentToken) <= currentToken;
}
