// Run against a frozen tx-submission-api checkout, never its live working tree.
// Production send journal, HTTP relayer client and refund guard; real PG/Anvil.
import http from 'node:http';
import { pathToFileURL } from 'node:url';
const root=process.env.HANDOFF_API_SNAPSHOT;
const load=path=>import(pathToFileURL(`${root}/${path}`));
const {migrateDb}=await load('test/pg/harness/db.ts');
await migrateDb(process.env.DATABASE_URL,`${root}/drizzle`);
const {db}=await load('src/db/index.ts');
const {claim,release}=await load('src/persistence/work-lease.ts');
const {journaledSend}=await load('src/persistence/send-journal.ts');
const {cancelEscrow,hasUnresolvedStepSend}=await load('src/persistence/escrow-repository.ts');
await db.$client.query(`INSERT INTO withdrawal_escrow(id,"user",token_address,amount,status,intent_id,step_index,expires_at) VALUES('handoff','0x000000000000000000000000000000000000dead','native',1,'blocked','handoff',0,statement_timestamp()-interval '1 hour')`);
const lease=await claim('step:handoff:0');
if(!lease) throw Error('lease missing');
const ctx={lease,refId:'handoff:0'};
const params={purpose:'step_send',chainId:1,actor:'solver',kind:'handoff',request:{to:'0x000000000000000000000000000000000000dead',data:'0x',value:424242n},label:'handoff payout'};
let outcome='not-started';
const server=http.createServer(async(req,res)=>{
 try {
  let value;
  if(req.url==='/start') { outcome='running';journaledSend(ctx,params).then(hash=>{outcome=hash},error=>{outcome=String(error)});value={started:true}; }
  else if(req.url==='/repeat') value={hash:await journaledSend(ctx,params)};
  else if(req.url==='/cancel') value={unresolved:await hasUnresolvedStepSend('handoff',0),cancelled:await cancelEscrow({escrowId:'handoff',reason:'timeout test',lease})};
  else value={outcome,journal:(await db.$client.query('SELECT * FROM send_journal')).rows,escrow:(await db.$client.query('SELECT status,spicenet_cancel_tx_hash FROM withdrawal_escrow')).rows};
  res.writeHead(200);res.end(JSON.stringify(value));
 }catch(error){res.writeHead(500);res.end(JSON.stringify({error:String(error)}));}
});
server.listen(Number(process.env.HANDOFF_API_PORT),'127.0.0.1');
process.on('SIGTERM',async()=>{server.close();await release(lease);await db.$client.end();process.exit(0)});
