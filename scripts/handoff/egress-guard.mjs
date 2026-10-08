// Test process: refuse all non-loopback HTTP, including accidental real RPCs.
import http from 'node:http';
import https from 'node:https';
const allowed=new Set(['127.0.0.1','localhost','::1','[::1]']);
const check=url=>{if(!allowed.has(new URL(url).hostname))throw Error('Non-local test egress refused')};
const original=globalThis.fetch;
globalThis.fetch=(url,options)=>{check(url instanceof Request?url.url:url);return original(url,options)};
for(const module of [http,https]){
 const request=module.request.bind(module);
 module.request=(url,...args)=>{const host=typeof url==='string'||url instanceof URL?new URL(url).hostname:url.hostname||url.host||'localhost';if(!allowed.has(host))throw Error('Non-local test egress refused');return request(url,...args)};
 module.get=(...args)=>{const req=module.request(...args);req.end();return req};
}
