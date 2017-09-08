import bluebird from 'bluebird';
import winston from 'winston';
import redis from 'redis';

bluebird.promisifyAll(redis.RedisClient.prototype);
const redisClient = redis.createClient();

const redisBase = {
  development: 3,
  test: 4,
  production: 5,
};

redisClient.select(redisBase[process.env.NODE_ENV]);

const Cache = {
  // Expire redis time by type
  redisCacheTime: {
    postStatistic: 60,
    default: 60 * 60,
  },

  async getFromCache(projectId, type, ...keys) {
    const redisTypeKey = `${projectId}_${type}`;
    const redisItemKey = this._getRedisItemKey(keys);
    winston.info(`Get from cache. ProjectId: ${projectId}. Type: ${type}. Key: ${redisItemKey || 'undefined or null'}`);

    // Get type timestamp
    const timestampType = await redisClient.getAsync(`${redisTypeKey}_timestamp`);
    winston.info(`Timestamp type: ${timestampType}`);
    if (!timestampType) {
      winston.info('Not found in cache');
      return false;
    }

    // Gen redis full key with timestamp. If haven't key than:
    let redisFullKey = `${redisTypeKey}_${timestampType}`;

    // If have key than:
    if (redisItemKey) {
      // Get timestamp for one item
      const timestampItem = await redisClient.getAsync(`${redisFullKey}_${redisItemKey}_timestamp`);
      winston.info(`Timestamp item: ${timestampItem}`);
      if (!timestampItem) {
        winston.info('Not found in cache');
        return false;
      }

      // Gen redis full key with timestamp
      redisFullKey = `${redisFullKey}_${redisItemKey}_${timestampItem}`;
    }

    // Get data
    winston.info(`Get data from redis full key: ${redisFullKey}`);
    const data = await redisClient.getAsync(redisFullKey);

    // Try parse data
    let dataParsed;
    if (data) {
      winston.info('Found in cache');
      try {
        dataParsed = JSON.parse(data);
      } catch (e) {
        winston.error(`Cache data not parsed! ${e}`);
      }
    } else {
      winston.info('Not found in cache');
    }

    return dataParsed;
  },

  async putToCache(data, projectId, type, ...keys) {
    const redisTypeKey = `${projectId}_${type}`;
    const redisItemKey = this._getRedisItemKey(keys);
    winston.info(`Put to cache. ProjectId: ${projectId}. Type: ${type}. Key: ${redisItemKey || 'undefined or null'}`);

    // Data is null or undefined
    if (!data) {
      winston.info('Data is', data);
      return false;
    }

    // Stringify data
    const dataStringified = JSON.stringify(data);

    // Get expire redis time by type
    const redisCacheTime = this.redisCacheTime[type] || this.redisCacheTime.default;

    // Update data and timestamp
    if (redisItemKey) {
      // Update item data and timestamp if we have key
      const { timestampType, newTimestamp } = await this._updateItemTimestamp(redisTypeKey, redisItemKey);

      // Set data for item
      const redisFullKey = `${redisTypeKey}_${timestampType}_${redisItemKey}_${newTimestamp}`;
      winston.info(`Set data to redis full key: ${redisFullKey}`);
      redisClient.setAsync(redisFullKey,
        dataStringified, 'EX', redisCacheTime,
      );
    } else {
      // Update type data and timestamp if we not have key

      // Set timestamp for type
      const newTimestamp = await this._updateTypeTimestamp(redisTypeKey);

      // Set data for type
      const redisFullKey = `${redisTypeKey}_${newTimestamp}`;
      winston.info(`Set data to redis full key: ${redisFullKey}`);
      redisClient.setAsync(redisFullKey,
        dataStringified, 'EX', redisCacheTime,
      );
    }
  },

  async clearFromCache(projectId, type, ...keys) {
    const redisTypeKey = `${projectId}_${type}`;
    const redisItemKey = this._getRedisItemKey(keys);
    winston.info(`Clear from cache. ProjectId: ${projectId}. Type: ${type}. Key: ${redisItemKey || 'undefined or null'}`);

    if (redisItemKey) {
      this._updateItemTimestamp(redisTypeKey, redisItemKey);
    } else {
      // Update timestamp for type
      this._updateTypeTimestamp(redisTypeKey);
    }
  },

  async _updateTypeTimestamp(redisTypeKey) {
    // Update timestamp for type
    const newTimestamp = Date.now();
    const redisFullTimestampKey = `${redisTypeKey}_timestamp`;
    winston.info(`Update redis full key timestamp: ${redisFullTimestampKey}`);
    redisClient.setAsync(redisFullTimestampKey, newTimestamp);

    return newTimestamp;
  },

  async _updateItemTimestamp(redisTypeKey, redisItemKey) {
    let timestampType = await redisClient.getAsync(`${redisTypeKey}_timestamp`);
    const newTimestamp = Date.now();

    if (timestampType) {
      winston.info(`Timestamp type: ${timestampType}`);
    } else {
      // Type timestamp is null, than set new timestamp for type
      timestampType = newTimestamp;
      redisClient.setAsync(`${redisTypeKey}_timestamp`, newTimestamp);
      winston.info(`Type timestamp not found! Update redis type timestamp: ${redisTypeKey}_timestamp`);
    }

    const redisFullTimestampKey = `${redisTypeKey}_${timestampType}_${redisItemKey}_timestamp`;
    winston.info(`Update redis full key timestamp: ${redisFullTimestampKey}`);
    redisClient.setAsync(redisFullTimestampKey, newTimestamp);

    return {
      newTimestamp,
      timestampType,
    };
  },

  _getRedisItemKey(keys) {
    return keys && keys.length
      ? keys.reduce((prev, current) => prev + (current && `_${current}`))
      : '';
  },
};

export default Cache;
