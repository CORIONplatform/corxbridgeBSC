import admin from "firebase-admin";
import credentials from "./credentials.json";
const db = admin.initializeApp({ credential: admin.credential.cert(credentials as any) }).firestore();

import { providers, Wallet, Contract, BigNumber, utils } from "ethers";
import Token from "./abis/Token.json";
import BridgeAssist from "./abis/BridgeAssist.json";
import walletPrivateKey from "./secret"; // 0x48d936dc37216a7d1B468a0b0426b37e68008eA9 wallet privateKey
import alchemyKey from "./secretAlchemy";
const address_BAB = "0xEFA7eAb30F3DdDFC3926F4083f319d7B6238BFD7"; // BSC Mainnet: 0xEFA7eAb30F3DdDFC3926F4083f319d7B6238BFD7 | BSC Testnet: 0xCA661795d34535dB71bB09e58fE32a5c654Ce7b8
const address_BAE = "0x7302b2F207b02Bcc2dea68C0950EB0Dc6C695b84"; // ETH Mainnet: 0x7302b2F207b02Bcc2dea68C0950EB0Dc6C695b84 | ETH Ropsten: 0xE5B9DD83e066650804fC28BCE24d372f09Fd5228
const address_TKNB = "0x36184181FA321E350aaAF88dad723E281365c1Ac"; // BSC Mainnet: 0x36184181FA321E350aaAF88dad723E281365c1Ac | BSC Testnet: 0x00EE45b4dED7df3B85a0305a5c7014c7dA455ad3
const address_TKNE = "0x26a604DFFE3ddaB3BEE816097F81d3C4a2A4CF97"; // ETH Mainnet: 0x26a604DFFE3ddaB3BEE816097F81d3C4a2A4CF97 | ETH Ropsten: 0x604B20031d473bfEc55f8546fA11b6E328E8f1e0
// [RPC] BSC Mainnet: https://bsc-dataseed.binance.org/ | BSC Testnet: https://data-seed-prebsc-2-s2.binance.org:8545/
const _RPCS = ["https://bsc-dataseed1.ninicoin.io/", "https://bsc-dataseed1.defibit.io/", "https://bsc-dataseed.binance.org/"]; // BSC RPCs sometimes lag and fail transactions
// const _RPCS = ["https://data-seed-prebsc-2-s2.binance.org:8545/", "https://data-seed-prebsc-2-s2.binance.org:8545/", "https://data-seed-prebsc-2-s2.binance.org:8545/"]; // for testnet setup
const providerB = new providers.JsonRpcProvider(_RPCS[2]); // for reading contracts
const providerE = new providers.AlchemyProvider(1, alchemyKey); // for reading contracts

// queues and buffer lifetime
const TIME_QUEUE = 120000;
const TIME_PARAMS = 30000;
const TIME_PRICE = 30000;

import bunyan from "bunyan";
import { LoggingBunyan } from "@google-cloud/logging-bunyan";
import axios from "axios";
const loggingBunyan = new LoggingBunyan();
const logger = bunyan.createLogger({ name: "my-service", streams: [loggingBunyan.stream("debug")] });

// Info Buffers
type DirectionType = "BE" | "EB";
type ChangeableParams = { CTF: number; FTM: number; FEE: number; PSD: boolean };
type ProbitDataType = { last: string; low: string; high: string; change: string; base_volume: string; quote_volume: string; market_id: string; time: string };
type PricesBuffer = { date: number; prices: { TU: number; BU: number; EU: number; TBP: number; TEP: number } };
type CostBuffer = { date: number; cost: BigNumber };
let paramsBuffer = { date: 0, params: { CTF: 200, FTM: 200, FEE: 13, PSD: false } as ChangeableParams };
let costBuffers = { BE: { date: 0, cost: BigNumber.from(0) }, EB: { date: 0, cost: BigNumber.from(0) } } as { [key in DirectionType]: CostBuffer };
let pricesBuffer = { date: 0, prices: { TU: 0, BU: 0, EU: 0, TBP: 0, TEP: 0 } } as PricesBuffer;

async function loadChangeableParams() {
  if (Date.now() - paramsBuffer.date < TIME_PARAMS) return paramsBuffer.params;
  try {
    const params = (await db.collection("config").doc("changeables").get()).data() as ChangeableParams | undefined;
    if (!params) throw new Error("Could not get config from firestore");
    paramsBuffer = { date: Date.now(), params };
    return params;
  } catch (error) {
    throw new Error(`Could not load params: ${error.reason || error.message}`);
  }
}
async function writeQueue(direction: DirectionType, address: string) {
  try {
    await db.collection(`queue${direction}`).doc(address).create({ date: Date.now() });
  } catch (error) {
    throw new Error(`Could not write to queue: ${error.reason || error.message}`);
  }
}
async function clearQueue(direction: DirectionType, address: string) {
  try {
    await db.collection(`queue${direction}`).doc(address).delete();
  } catch (error) {
    throw new Error(`Could not clear queue: ${error.reason || error.message}`);
  }
}
async function assertQueue(direction: DirectionType, address: string) {
  let entry: any;
  try {
    entry = (await db.collection(`queue${direction}`).doc(address).get()).data();
  } catch (error) {
    throw new Error(`Could not check request queue: ${error.reason || error.message}`);
  }
  if (entry) {
    if (Date.now() - entry.date < TIME_QUEUE) throw new Error(`Request done recently: timeout is ${TIME_QUEUE}ms`);
    else await db.collection(`queue${direction}`).doc(address).delete(); // if it was left undeleted since last time
  }
}
async function getPrices() {
  if (Date.now() - pricesBuffer.date < TIME_PRICE) return pricesBuffer.prices;
  try {
    const res = (await axios.get(`https://api.probit.com/api/exchange/v1/ticker?market_ids=CORX-USDT,BNB-USDT,ETH-USDT`)).data as {
      data: [ProbitDataType, ProbitDataType, ProbitDataType];
    };
    if (!res.data?.length) throw new Error("No such pairs");
    const TU = Number(res.data[0].last);
    const BU = Number(res.data[1].last);
    const EU = Number(res.data[2].last);
    const TEP = TU / EU;
    const TBP = TU / BU;
    const prices = { TU, BU, EU, TBP, TEP };
    pricesBuffer = { date: Date.now(), prices };
    return prices;
  } catch (error) {
    throw new Error(`Could not get CORX price: ${error.mssage}`);
  }
}
// Calculate Cost
function _calcCost(gas: BigNumber, gasPrice: BigNumber, tknPrice: number) {
  return gasPrice
    .mul(gas)
    .mul(1e8)
    .div(Math.trunc(tknPrice * 1e8))
    .div(1e10); // decimals() difference between CORX and ETH/BNB is 10
}
function calcCost(BG: BigNumber, BGP: BigNumber, TBP: number, EG: BigNumber, EGP: BigNumber, TEP: number) {
  return _calcCost(BG, BGP, TBP).add(_calcCost(EG, EGP, TEP));
}
// Estimate Cost
async function estimateCost(direction: DirectionType) {
  if (Date.now() - Number(costBuffers[direction].date) < TIME_PRICE) return costBuffers[direction].cost;
  const _GPs = { BE: [BigNumber.from(26000), BigNumber.from(58000)], EB: [BigNumber.from(72000), BigNumber.from(54000)] }; // [BGas, EGas]
  try {
    const [BGP, EGP, { TBP, TEP }] = await Promise.all([providerB.getGasPrice(), providerE.getGasPrice(), getPrices()]);
    const cost = calcCost(BigNumber.from(_GPs[direction][0]), BGP, TBP, BigNumber.from(_GPs[direction][1]), EGP, TEP);
    costBuffers[direction] = { date: Date.now(), cost };
    return cost;
  } catch (error) {
    throw new Error(`Could not estimate cost: ${error.reason || error.message}`);
  }
}
// Estimate Fees applied
async function estimateFee(direction: DirectionType) {
  try {
    const [cost, { CTF }] = await Promise.all([estimateCost(direction), loadChangeableParams()]);
    return cost.mul(CTF).div(100);
  } catch (error) {
    throw new Error(`Could not estimate fee: ${error.message}`);
  }
}
// Check safety of following swap attempt
async function assureSafety(direction: DirectionType, address: string): Promise<{ allowance: BigNumber; balance: BigNumber; fee: BigNumber }> {
  try {
    const _provider = direction === "BE" ? providerB : providerE;
    const _address_TKN = { BE: address_TKNB, EB: address_TKNE }[direction];
    const _address_BA = { BE: address_BAB, EB: address_BAE }[direction];
    const TKN = new Contract(_address_TKN, Token.abi, _provider);
    const [allowance, balance, _fee, params]: [BigNumber, BigNumber, BigNumber, ChangeableParams] = await Promise.all([
      TKN.allowance(address, _address_BA),
      TKN.balanceOf(address),
      estimateFee(direction),
      loadChangeableParams(),
    ]);
    const fee = allowance.mul(params.FEE).div(10000).add(_fee);
    const min = _fee.mul(params.FTM).div(100);
    logger.debug(`assureSafety(): [direction]:${direction}|[address]:${address}|[allowance]:${allowance}|[balance]:${balance}|[_fee]:${_fee}|${fee}`);
    if (allowance.lt(min)) throw new Error(`Amount is too low. Should be not less than ${BigNumber.from(min).div(1e8).toString()} CORX`);
    if (allowance.gt(balance)) throw new Error(`Actual balance (${balance.toString()}) is lower than allowance (${allowance.toString()})`);
    return { allowance, balance, fee };
  } catch (error) {
    throw new Error(`Assertion failure: ${error.reason || error.message}`);
  }
}
// Process requests
async function _collect(direction: DirectionType, address: string, amount: BigNumber) {
  const _provider = direction === "BE" ? providerB : providerE;
  const _signer = new Wallet(walletPrivateKey, _provider);
  const _address_BA = { BE: address_BAB, EB: address_BAE }[direction];
  const _BA = new Contract(_address_BA, BridgeAssist.abi, _signer);
  let tx, receipt: any;
  let err: Error | undefined;
  try {
    tx = await _BA.collect(address, amount);
    receipt = await tx.wait();
    logger.debug(`_collect() ${receipt.transactionHash} | GAS ${receipt.gasUsed} | GL ${tx.gasLimit} | GP ${tx.gasPrice}`);
  } catch (error) {
    err = new Error(`[reason]:${error.reason}|[tx]:${[tx?.nonce, tx?.hash]}|[receipt.confirmations]:${receipt?.confirmations}|[message]:${error.message}`);
    logger.warn(`_collect(${direction[0]}) failure... Info: [${err.message}]`);
  }
  return { tx, receipt, err };
}
function _wait(ms = 5000) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function _dispense(
  direction: DirectionType,
  address: string,
  amount: BigNumber,
  retriesLeft = 2
): Promise<{ tx: any; receipt: any; err: Error | undefined }> {
  const _provider = direction === "BE" ? providerE : new providers.JsonRpcProvider(_RPCS[retriesLeft]);
  const _signer = new Wallet(walletPrivateKey, _provider);
  const _address_BA = { BE: address_BAE, EB: address_BAB }[direction];
  const _BA = new Contract(_address_BA, BridgeAssist.abi, _signer);
  let tx, receipt: any;
  let err: Error | undefined;
  try {
    tx = await _BA.dispense(address, amount);
    receipt = await tx.wait();
    logger.debug(`_dispense() ${receipt.transactionHash} | GAS ${receipt.gasUsed} | GL ${tx.gasLimit} | GP ${tx.gasPrice}`);
  } catch (error) {
    err = new Error(`[reason]:${error.reason}|[tx]:${[tx?.nonce, tx?.hash]}|[receipt.confirmations]:${receipt?.confirmations}|[message]:${error.message}`);
    logger.warn(`_dispense(${direction[1]}) failure... Retries left: ${retriesLeft} | Info: [${err.message}]`);
    if (retriesLeft && !tx) {
      await _wait();
      return await _dispense(direction, address, amount, retriesLeft - 1);
    }
  }
  return { tx, receipt, err };
}
async function processRequest(direction: DirectionType, address: string) {
  let txHashCollect: string | undefined, txHashDispense: string | undefined;
  let err: Error | undefined;
  try {
    await assertQueue(direction, address);
    await writeQueue(direction, address);
    const { allowance, fee } = await assureSafety(direction, address);
    const resC = await _collect(direction, address, allowance);
    if (resC.err) throw new Error(`Could not collect: ${resC.err.message}`);
    txHashCollect = resC.receipt.transactionHash as string;
    const resD = await _dispense(direction, address, allowance.sub(fee));
    if (resD.err) throw new Error(`Could not dispense: ${resD.err.message}`);
    txHashDispense = resD.receipt.transactionHash as string;
    try {
      await clearQueue(direction, address);
    } catch (error) {
      logger.warn(`clearQueue() failure... Error: ${error.message}`);
    }
  } catch (error) {
    err = new Error(`Could not process request: ${error.message}`);
  }
  return { txHashCollect, txHashDispense, err };
}

const express = require("express");
const cors = require("cors");
const app = express();
app.use(cors());
app.get("/process", async (req: any, res: any) => {
  const direction = typeof req.query.direction === "string" ? (req.query.direction.toUpperCase() as DirectionType) : undefined;
  const address = typeof req.query.address === "string" ? req.query.address.toLowerCase() : undefined;
  let dispenseFailure: false | string = false;
  try {
    if (!direction || !["BE", "EB"].includes(direction)) throw new Error("Invalid query: 'direction' must be 'BE' or 'EB'");
    if (!utils.isAddress(address || "") || address === "0x0000000000000000000000000000000000000000") throw new Error("Invalid query: 'address'");
  } catch (error) {
    res.status(400).send(error.message);
    return;
  }
  const _prefix = `[${direction}][${{ BE: "CORXb to CORX", EB: "CORX to CORXb" }[direction]}][${address}]`;
  try {
    logger.info(`${_prefix}: Incoming request`);
    const result = await processRequest(direction, address!);
    if (result.err) {
      // if asset was collected but not dispensed
      if (result.txHashCollect && !result.txHashDispense) dispenseFailure = result.txHashCollect;
      throw result.err;
    }
    logger.info(`${_prefix}: Success. Collect: ${result.txHashCollect}, Dispense: ${result.txHashDispense}`);
    res.status(200).send({ txHashCollect: result.txHashCollect, txHashDispense: result.txHashDispense });
  } catch (error) {
    logger.error(`${_prefix}: Failed. Error: ${error.message}`);
    logger.error(`[error.reason]: [${error.reason}]`);
    if (dispenseFailure) {
      // if asset was collected but not dispensed
      logger.fatal(`!!DISPENSE FAILED AFTER SUCCESSFUL COLLECT. TX HASH: [${dispenseFailure}]`);
      // only in that case response status is 500
      res
        .status(500)
        .send(
          "WARNING! Asset was collected but due to internal server error it wasn't dispensed to you on another blockchain. " +
            "Administrator shall soon receive automatic message and dispense manually. Or you can contact the support right now. | " +
            `collect() transaction hash: [${dispenseFailure}] | ` +
            `Error returned: ${error.reason || error.message}`
        );
    } else {
      res.status(400).send(error.reason || error.message);
    }
  }
});
app.get("/info/costs", async (req: any, res: any) => {
  try {
    const [BE, EB] = (await Promise.all([estimateCost("BE"), estimateCost("EB")])).map((c) => c.toString());
    res.status(200).send({ BE, EB });
  } catch (error) {
    res.status(400).send(error.reason || error.message);
  }
});
const port = process.env.PORT || 3001;
app.listen(port, () => {
  logger.info(`Express app listening at port ${port}`);
});
