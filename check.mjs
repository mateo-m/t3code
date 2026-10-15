import net from "node:net";
const tryConnect = (host, port) => new Promise((r) => { const s = net.connect({ host, port }, () => { s.destroy(); r("ok"); }); s.on("error", (e) => r(e.code)); });
for (const bind of ["localhost", "::1", "127.0.0.1", "0.0.0.0", "::"]) {
  const srv = net.createServer((c) => c.end());
  await new Promise((r) => srv.listen(0, bind, r));
  const { address, port } = srv.address();
  console.log(`bind=${bind} -> ${address}  127.0.0.1:${await tryConnect("127.0.0.1", port)}  ::1:${await tryConnect("::1", port)}`);
  await new Promise((r) => srv.close(r));
}
