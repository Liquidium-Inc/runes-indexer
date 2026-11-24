use self::rune_updater::RuneUpdater;
use super::*;
use crate::index::reorg::Reorg;
use crate::into_usize::IntoUsize;
use crate::logs::{CRITICAL, INFO, WARNING};
use crate::timestamp;
use candid::Principal;
use ic_cdk_timers::TimerId;
use std::cell::Cell;
use std::collections::HashSet;
use std::time::Duration;

mod rune_updater;

pub(crate) struct BlockData {
  pub(crate) header: Header,
  pub(crate) txdata: Vec<(Transaction, Txid)>,
}

impl From<Block> for BlockData {
  fn from(block: Block) -> Self {
    BlockData {
      header: block.header,
      txdata: block
        .txdata
        .into_iter()
        .map(|transaction| {
          let txid = transaction.compute_txid();
          (transaction, txid)
        })
        .collect(),
    }
  }
}

/// How often the loop wakes up. Also the wait after a successful block.
const POLL_INTERVAL: Duration = Duration::from_secs(10);
/// Longest wait between retries after consecutive failures.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(640);
/// An attempt that has not finished after this long is treated as dead (it trapped after
/// its first await) and a new one may start.
const ATTEMPT_LEASE: Duration = Duration::from_secs(900);

thread_local! {
  /// The repeating timer that drives the loop. Repeating timers are rescheduled even when
  /// their callback traps, so a trap can't stop indexing.
  static LOOP_TIMER: RefCell<Option<TimerId>> = const { RefCell::new(None) };
  static SCHEDULER: RefCell<Scheduler> = RefCell::new(Scheduler::default());
  /// Bumped on every change to the index (block applied or rolled back). Work prepared
  /// against an older version is thrown away.
  static INDEX_VERSION: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn retry_delay(consecutive_failures: u32) -> Duration {
  if consecutive_failures == 0 {
    return POLL_INTERVAL;
  }
  POLL_INTERVAL
    .saturating_mul(1 << consecutive_failures.min(6))
    .min(MAX_RETRY_DELAY)
}

/// Decides when an attempt may start. Times are IC nanoseconds.
#[derive(Default, Debug)]
pub(crate) struct Scheduler {
  failures: u32,
  next_attempt_at: u64,
  /// (attempt id, start time) of the attempt that owns the loop.
  in_flight: Option<(u64, u64)>,
  last_id: u64,
}

impl Scheduler {
  /// Starts an attempt if none is running (or the running one outlived its lease) and its
  /// backoff has passed. The attempt is counted as failed until `finish` says otherwise,
  /// so a trap later in the attempt still backs off.
  pub(crate) fn begin(&mut self, now: u64) -> Option<u64> {
    if let Some((_, started)) = self.in_flight {
      if now < started.saturating_add(ATTEMPT_LEASE.as_nanos() as u64) {
        return None;
      }
    }
    if now < self.next_attempt_at {
      return None;
    }
    self.failures = self.failures.saturating_add(1);
    self.next_attempt_at = now.saturating_add(retry_delay(self.failures).as_nanos() as u64);
    self.last_id += 1;
    self.in_flight = Some((self.last_id, now));
    Some(self.last_id)
  }

  /// Ends an attempt. Returns false, and changes nothing, if a newer attempt took over.
  pub(crate) fn finish(&mut self, id: u64, succeeded: bool) -> bool {
    if self.in_flight.map(|(current, _)| current) != Some(id) {
      return false;
    }
    self.in_flight = None;
    if succeeded {
      self.failures = 0;
      self.next_attempt_at = 0;
    }
    true
  }

  pub(crate) fn idle(&self) -> bool {
    self.in_flight.is_none()
  }

  pub(crate) fn retry_in(&self, now: u64) -> Duration {
    Duration::from_nanos(self.next_attempt_at.saturating_sub(now))
  }
}

/// Starts (or restarts) the index loop. There is only ever one loop timer.
pub fn update_index(network: BitcoinNetwork, subscribers: Vec<Principal>) -> Result {
  stop_loop();
  let id =
    ic_cdk_timers::set_timer_interval(POLL_INTERVAL, move || tick(network, subscribers.clone()));
  LOOP_TIMER.with(|t| *t.borrow_mut() = Some(id));
  Ok(())
}

fn stop_loop() {
  if let Some(id) = LOOP_TIMER.with(|t| t.borrow_mut().take()) {
    ic_cdk_timers::clear_timer(id);
  }
}

fn tick(network: BitcoinNetwork, subscribers: Vec<Principal>) {
  if is_shutting_down() {
    if SCHEDULER.with_borrow(|s| s.idle()) {
      log!(INFO, "shutting down index thread");
      stop_loop();
    }
    return;
  }

  let Some(id) = SCHEDULER.with_borrow_mut(|s| s.begin(ic_cdk::api::time())) else {
    return;
  };

  ic_cdk::spawn(async move {
    let outcome = index_next_block(network, &subscribers).await;
    if !SCHEDULER.with_borrow_mut(|s| s.finish(id, outcome.is_ok())) {
      log!(
        INFO,
        "attempt {id} finished after a newer one took over; ignored"
      );
      return;
    }
    match outcome {
      Ok(Step::Done) => {}
      Ok(Step::Halt(reason)) => {
        log!(
          CRITICAL,
          "{reason}; indexing halted until a controller fixes this and calls start"
        );
        stop_loop();
      }
      Err(e) => {
        let retry_in = SCHEDULER.with_borrow(|s| s.retry_in(ic_cdk::api::time()));
        log_critical_once(format!("{e:#} (retrying in {}s)", retry_in.as_secs()));
      }
    }
  });
}

fn log_critical_once(message: String) {
  let is_new_message = CRITICAL.with_borrow(|sink| {
    sink
      .iter()
      .last()
      .map_or(true, |entry| entry.message != message)
  });
  if is_new_message {
    log!(CRITICAL, "{}", message);
  }
}

enum Step {
  Done,
  /// Retrying cannot help; a controller has to act.
  Halt(String),
}

async fn index_next_block(network: BitcoinNetwork, subscribers: &[Principal]) -> Result<Step> {
  let version = INDEX_VERSION.get();
  let (height, index_prev_blockhash) = crate::index::next_block(network);

  // Upstream wrote these statistics before the block's first RPC call and the header only
  // at the end. An entry above the tip means a block was left partly applied; retrying
  // on top of it would count it twice. Restore a clean snapshot instead.
  if crate::index::mem_has_statistics_at(height) {
    return Ok(Step::Halt(format!(
      "block {height} was partly applied by an earlier version"
    )));
  }

  let Some(block_hash) = crate::bitcoin_api::get_block_hash(network, height)
    .await
    .map_err(|e| anyhow!("failed to get_block_hash at height {height}: {e:?}"))?
  else {
    return Ok(Step::Done);
  };

  let block = crate::rpc::get_block(block_hash)
    .await
    .map_err(|e| anyhow!("failed to get_block: {block_hash:?} error: {e:?}"))?;

  match Reorg::detect_reorg(
    network,
    index_prev_blockhash,
    block.header.prev_blockhash,
    height,
  )
  .await
  {
    Ok(()) => {}
    Err(reorg::Error::Recoverable { height, depth }) => {
      ensure_unchanged(version, height)?;
      Reorg::handle_reorg(height, depth);
      INDEX_VERSION.set(version + 1);
      return Ok(Step::Done);
    }
    Err(reorg::Error::Retry) => {
      return Err(anyhow!(
        "could not fetch a block hash while checking a reorg at height {height}"
      ));
    }
    Err(reorg::Error::Unrecoverable) => {
      return Ok(Step::Halt(format!(
        "unrecoverable reorg detected at height {height}"
      )));
    }
  }

  // All RPC calls the block needs happen here, before any state changes.
  let commitments = prefetch_commitments(&RpcCommitLookup, network, height, &block)
    .await
    .map_err(|e| anyhow!("failed to index_block at height {height}: {e:?}"))?;

  // From here to the end of index_block there is no await, so the IC commits the whole
  // block together with its header, or none of it if anything traps.
  ensure_unchanged(version, height)?;
  let txids: Vec<String> = block
    .txdata
    .iter()
    .map(|(_, txid)| txid.to_string())
    .collect();
  let block_timestamp = block.header.time as u64;
  if let Err(e) = index_block(network, height, &block, commitments) {
    // Trapping rolls back every write of this block. The attempt is already counted as
    // failed and holds a lease, so the loop retries after the lease with backoff.
    ic_cdk::trap(&format!("failed to index_block at height {height}: {e:?}"));
  }
  Reorg::prune_change_record(network, height);
  INDEX_VERSION.set(version + 1);

  for subscriber in subscribers.iter() {
    let _ = crate::notifier::notify_new_block(
      *subscriber,
      height,
      block_hash.to_string(),
      block_timestamp,
      txids.clone(),
    )
    .await;
    log!(
      INFO,
      "notified subscriber: {:?} with block_height: {:?} block_hash: {:?} block_timestamp: {:?}",
      subscriber.to_text(),
      height,
      block_hash,
      block_timestamp
    );
  }

  Ok(Step::Done)
}

/// Attempts only overlap when one outlives its lease. Only an attempt whose view of the
/// index is still current may write.
fn ensure_unchanged(version: u64, height: u32) -> Result {
  if INDEX_VERSION.get() != version {
    return Err(anyhow!(
      "index changed while block {height} was being fetched; skipping"
    ));
  }
  Ok(())
}

fn bitcoin_network(network: BitcoinNetwork) -> bitcoin::Network {
  match network {
    BitcoinNetwork::Mainnet => bitcoin::Network::Bitcoin,
    BitcoinNetwork::Testnet => bitcoin::Network::Testnet4,
    BitcoinNetwork::Regtest => bitcoin::Network::Regtest,
  }
}

/// Looks up the output an etching's commit input spends.
pub(crate) trait CommitLookup {
  /// `None` if the output is not P2TR, otherwise the height of the block that confirmed it.
  async fn taproot_output_height(&self, outpoint: OutPoint) -> Result<Option<u32>>;
}

struct RpcCommitLookup;

impl CommitLookup for RpcCommitLookup {
  async fn taproot_output_height(&self, outpoint: OutPoint) -> Result<Option<u32>> {
    let tx_info = crate::rpc::get_raw_transaction_info(&outpoint.txid, None).await?;

    let taproot = tx_info
      .vout
      .get(outpoint.vout.into_usize())
      .ok_or_else(|| {
        anyhow!(
          "Transaction {} has no output {}",
          tx_info.txid,
          outpoint.vout
        )
      })?
      .script_pub_key
      .script()?
      .is_p2tr();

    if !taproot {
      return Ok(None);
    }

    let block_hash = tx_info.blockhash.ok_or_else(|| {
      log!(WARNING, "Transaction {} has no block hash", tx_info.txid);
      anyhow!("Transaction {} has no block hash", tx_info.txid)
    })?;

    let height = crate::rpc::get_block_header_info(&block_hash).await?.height;

    Ok(Some(u32::try_from(height)?))
  }
}

/// Runs, for every etching in the block that could need it, the commitment check that
/// needs RPC calls. Reads state but never writes it. Keyed by transaction index.
pub(crate) async fn prefetch_commitments(
  lookup: &impl CommitLookup,
  network: BitcoinNetwork,
  height: u32,
  block: &BlockData,
) -> Result<HashMap<u32, bool>> {
  let minimum = Rune::minimum_at_height(bitcoin_network(network), Height(height));
  let mut commitments = HashMap::new();
  // Runes this block will etch. `etched` skips a later etching of the same name because
  // the rune exists by then, so its commitment is never needed.
  let mut etched_in_block = HashSet::new();

  for (tx_index, (tx, _)) in block.txdata.iter().enumerate() {
    let rune = match Runestone::decipher(tx) {
      Some(Artifact::Runestone(runestone)) => runestone.etching.and_then(|etching| etching.rune),
      Some(Artifact::Cenotaph(cenotaph)) => cenotaph.etching,
      None => None,
    };

    let Some(rune) = rune else {
      continue;
    };

    // The checks `etched` makes before it needs the commitment. Runes are never removed
    // while a block is applied, so a rune that exists now also exists then.
    if rune < minimum
      || rune.is_reserved()
      || etched_in_block.contains(&rune.0)
      || crate::index::mem_get_rune_to_rune_id(rune.store()).is_some()
    {
      continue;
    }

    let commits = tx_commits_to_rune(lookup, height, tx, rune).await?;
    if commits {
      etched_in_block.insert(rune.0);
    }
    commitments.insert(u32::try_from(tx_index).unwrap(), commits);
  }

  Ok(commitments)
}

async fn tx_commits_to_rune(
  lookup: &impl CommitLookup,
  height: u32,
  tx: &Transaction,
  rune: Rune,
) -> Result<bool> {
  let commitment = rune.commitment();

  for input in &tx.input {
    // extracting a tapscript does not indicate that the input being spent
    // was actually a taproot output. this is checked below, when we load the
    // output's entry from the database
    let Some(tapscript) = input.witness.tapscript() else {
      continue;
    };

    for instruction in tapscript.instructions() {
      // ignore errors, since the extracted script may not be valid
      let Ok(instruction) = instruction else {
        break;
      };

      let Some(pushbytes) = instruction.push_bytes() else {
        continue;
      };

      if pushbytes.as_bytes() != commitment {
        continue;
      }

      let Some(commit_tx_height) = lookup.taproot_output_height(input.previous_output).await?
      else {
        continue;
      };

      let confirmations = height.checked_sub(commit_tx_height).ok_or_else(|| {
        anyhow!("commit transaction at height {commit_tx_height} is above block {height}")
      })?
        + 1;

      if confirmations >= Runestone::COMMIT_CONFIRMATIONS as u32 {
        return Ok(true);
      }
    }
  }

  Ok(false)
}

/// Applies one block. Synchronous on purpose: with no await inside, a trap anywhere in it
/// rolls back the whole block.
pub(crate) fn index_block(
  network: BitcoinNetwork,
  height: u32,
  block: &BlockData,
  commitments: HashMap<u32, bool>,
) -> Result<()> {
  log!(
    INFO,
    "Block {} at {} with {} transactions…",
    height,
    timestamp(block.header.time.into()),
    block.txdata.len()
  );

  let runes = crate::index::mem_statistic_runes();
  let reserved_runes = crate::index::mem_statistic_reserved_runes();

  if height % 10 == 0 {
    log!(
      INFO,
      "Index statistics at height {}: latest_block: {:?}, reserved_runes: {}, runes: {}, rune_to_rune_id: {}, rune_entry: {}, transaction_id_to_rune: {}, outpoint_to_rune_balances: {}, outpoint_to_height: {}",
      height,
      crate::index::mem_latest_block(),
      reserved_runes,
      runes,
      crate::index::mem_length_rune_to_rune_id(),
      crate::index::mem_length_rune_id_to_rune_entry(),
      crate::index::mem_length_transaction_id_to_rune(),
      crate::index::mem_length_outpoint_to_rune_balances(),
      crate::index::mem_length_outpoint_to_height(),
    );
  }

  // init statistic runes/reserved_runes for new height
  crate::index::mem_insert_statistic_runes(height, runes);
  crate::index::mem_insert_statistic_reserved_runes(height, reserved_runes);

  let mut rune_updater = RuneUpdater {
    block_time: block.header.time,
    burned: HashMap::new(),
    commitments,
    height,
    minimum: Rune::minimum_at_height(bitcoin_network(network), Height(height)),
    runes,
    change_record: ChangeRecord::new(),
  };

  for (i, (tx, txid)) in block.txdata.iter().enumerate() {
    rune_updater.index_runes(u32::try_from(i).unwrap(), tx, *txid)?;
  }

  rune_updater.update()?;

  crate::index::mem_insert_block_header(height, block.header.store());

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::index::entry::RuneBalance;
  use bitcoin::{
    absolute::LockTime, block::Version, hashes::Hash, opcodes, script::Builder, transaction,
    Amount, CompactTarget, ScriptBuf, Sequence, TxIn, TxMerkleNode, TxOut, Witness,
  };
  use std::future::Future;
  use std::pin::pin;
  use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

  const NETWORK: BitcoinNetwork = BitcoinNetwork::Mainnet;
  const HEIGHT: u32 = 840_010;
  const MINTED: RuneId = RuneId {
    block: 840_000,
    tx: 1,
  };
  const ETCHED: Rune = Rune(1_000_000_000_000_000_000);

  /// The lookups below never wait, so polling once is enough.
  fn block_on<F: Future>(future: F) -> F::Output {
    fn raw() -> RawWaker {
      fn clone(_: *const ()) -> RawWaker {
        raw()
      }
      fn noop(_: *const ()) {}
      static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
      RawWaker::new(std::ptr::null(), &VTABLE)
    }
    let waker = unsafe { Waker::from_raw(raw()) };
    match pin!(future).poll(&mut Context::from_waker(&waker)) {
      Poll::Ready(output) => output,
      Poll::Pending => panic!("lookup future did not complete"),
    }
  }

  struct FailingRpc;
  impl CommitLookup for FailingRpc {
    async fn taproot_output_height(&self, _: OutPoint) -> Result<Option<u32>> {
      Err(anyhow!("getrawtransaction: 502 Bad Gateway"))
    }
  }

  struct WorkingRpc;
  impl CommitLookup for WorkingRpc {
    async fn taproot_output_height(&self, _: OutPoint) -> Result<Option<u32>> {
      Ok(Some(HEIGHT - 10))
    }
  }

  fn outpoint(n: u8, vout: u32) -> OutPoint {
    OutPoint {
      txid: Txid::from_byte_array([n; 32]),
      vout,
    }
  }

  fn tx(inputs: Vec<TxIn>, runestone: Runestone) -> Transaction {
    Transaction {
      version: transaction::Version::TWO,
      lock_time: LockTime::ZERO,
      input: inputs,
      output: vec![
        TxOut {
          value: Amount::from_sat(546),
          script_pubkey: ScriptBuf::new_p2wpkh(&bitcoin::WPubkeyHash::all_zeros()),
        },
        TxOut {
          value: Amount::ZERO,
          script_pubkey: runestone.encipher(),
        },
      ],
    }
  }

  fn input(previous_output: OutPoint, witness: Witness) -> TxIn {
    TxIn {
      previous_output,
      script_sig: ScriptBuf::new(),
      sequence: Sequence::MAX,
      witness,
    }
  }

  /// State before the block: rune MINTED is open for minting, and `holder` holds 500 of it.
  fn seed(holder: OutPoint) {
    crate::index::mem_insert_rune_id_to_rune_entry(
      MINTED.store(),
      RuneEntry {
        block: MINTED.block,
        burned: 0,
        divisibility: 0,
        etching: Txid::all_zeros(),
        mints: 0,
        number: 1,
        premine: 0,
        spaced_rune: SpacedRune {
          rune: Rune(2_000_000_000_000_000_000),
          spacers: 0,
        },
        symbol: None,
        terms: Some(Terms {
          amount: Some(100),
          cap: Some(1_000),
          height: (None, None),
          offset: (None, None),
        }),
        timestamp: 0,
        turbo: false,
      },
    );
    crate::index::mem_insert_statistic_runes(HEIGHT - 1, 2);
    crate::index::mem_insert_outpoint_to_rune_balances(
      holder.store(),
      RuneBalances {
        balances: vec![RuneBalance {
          rune_id: MINTED,
          balance: 500,
        }],
      },
    );
    crate::index::mem_insert_outpoint_to_height(holder.store(), HEIGHT - 5);
  }

  /// The reporter's block: tx 0 mints and spends a rune outpoint, tx 1 etches a rune whose
  /// commitment check needs RPC. In the old code tx 0 was written before tx 1's RPC call.
  fn reporter_block(holder: OutPoint) -> BlockData {
    let mint = tx(
      vec![input(holder, Witness::new())],
      Runestone {
        mint: Some(MINTED),
        ..Default::default()
      },
    );
    let commitment = Builder::new()
      .push_slice(<&bitcoin::script::PushBytes>::try_from(ETCHED.commitment().as_slice()).unwrap())
      .push_opcode(opcodes::all::OP_DROP)
      .into_script();
    let etch = tx(
      vec![input(
        outpoint(9, 0),
        Witness::from_slice(&[commitment.as_bytes(), &[0xc0; 33]]),
      )],
      Runestone {
        etching: Some(Etching {
          rune: Some(ETCHED),
          ..Default::default()
        }),
        ..Default::default()
      },
    );
    BlockData::from(Block {
      header: Header {
        version: Version::ONE,
        prev_blockhash: BlockHash::all_zeros(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 1_700_000_000,
        bits: CompactTarget::from_consensus(0),
        nonce: 0,
      },
      txdata: vec![mint, etch],
    })
  }

  fn mints() -> u128 {
    crate::index::mem_get_rune_id_to_rune_entry(MINTED.store())
      .unwrap()
      .mints
  }

  fn balance(outpoint: OutPoint) -> Option<u128> {
    crate::index::mem_get_outpoint_to_rune_balances(outpoint.store())
      .map(|balances| balances.balances.iter().map(|b| b.balance).sum())
  }

  #[test]
  fn failed_rpc_leaves_state_untouched_and_retry_counts_the_block_once() {
    let holder = outpoint(7, 0);
    seed(holder);
    let block = reporter_block(holder);
    let mint_output = OutPoint {
      txid: block.txdata[0].1,
      vout: 0,
    };

    // First attempt: the etching's RPC lookup fails.
    let error = block_on(prefetch_commitments(&FailingRpc, NETWORK, HEIGHT, &block));
    assert!(error.is_err());
    assert_eq!(
      mints(),
      0,
      "mint must not be counted before the block applies"
    );
    assert_eq!(
      balance(holder),
      Some(500),
      "spent outpoint must not be removed"
    );
    assert_eq!(balance(mint_output), None);
    assert_eq!(crate::index::mem_latest_block_height(), None);

    // Retry of the same height succeeds.
    let commitments = block_on(prefetch_commitments(&WorkingRpc, NETWORK, HEIGHT, &block)).unwrap();
    assert_eq!(commitments, HashMap::from([(1, true)]));
    index_block(NETWORK, HEIGHT, &block, commitments).unwrap();

    assert_eq!(mints(), 1, "mint counted exactly once");
    assert_eq!(balance(holder), None);
    assert_eq!(balance(mint_output), Some(600));
    assert!(crate::index::mem_get_rune_to_rune_id(ETCHED.store()).is_some());
    assert_eq!(crate::index::mem_latest_block_height(), Some(HEIGHT));
  }

  #[test]
  fn prefetch_skips_runes_that_already_exist() {
    let holder = outpoint(7, 0);
    seed(holder);
    crate::index::mem_insert_rune_to_rune_id(ETCHED.store(), RuneId { block: 1, tx: 1 }.store());
    let block = reporter_block(holder);

    // No RPC call is made, so the failing lookup is never reached.
    let commitments = block_on(prefetch_commitments(&FailingRpc, NETWORK, HEIGHT, &block)).unwrap();
    assert!(commitments.is_empty());
  }

  #[test]
  fn index_block_refuses_an_etching_without_its_commitment_check() {
    let holder = outpoint(7, 0);
    seed(holder);
    let block = reporter_block(holder);

    assert!(index_block(NETWORK, HEIGHT, &block, HashMap::new()).is_err());
  }

  #[test]
  fn retry_delay_backs_off_and_caps() {
    assert_eq!(retry_delay(0), Duration::from_secs(10));
    assert_eq!(retry_delay(1), Duration::from_secs(20));
    assert_eq!(retry_delay(3), Duration::from_secs(80));
    assert_eq!(retry_delay(6), MAX_RETRY_DELAY);
    assert_eq!(retry_delay(u32::MAX), MAX_RETRY_DELAY);
    assert!(ATTEMPT_LEASE > MAX_RETRY_DELAY);
  }

  const SEC: u64 = 1_000_000_000;

  #[test]
  fn scheduler_runs_one_attempt_at_a_time_and_backs_off() {
    let mut s = Scheduler::default();
    let first = s.begin(0).unwrap();
    assert_eq!(
      s.begin(10 * SEC),
      None,
      "no second attempt while one is running"
    );

    assert!(s.finish(first, false));
    assert_eq!(s.begin(10 * SEC), None, "backoff after one failure is 20 s");
    let second = s.begin(20 * SEC).unwrap();
    assert!(s.finish(second, false));
    assert_eq!(s.begin(50 * SEC), None, "40 s after two failures");
    let third = s.begin(60 * SEC).unwrap();

    assert!(s.finish(third, true));
    assert!(s.begin(60 * SEC).is_some(), "success clears the backoff");
  }

  #[test]
  fn scheduler_recovers_from_an_attempt_that_trapped() {
    let mut s = Scheduler::default();
    let dead = s.begin(0).unwrap(); // traps after its first await and never finishes
    let lease = ATTEMPT_LEASE.as_secs() * SEC;
    assert_eq!(s.begin(lease - 1), None);
    let next = s.begin(lease).unwrap();

    // The dead attempt's late completion must not touch the new attempt's state.
    assert!(!s.finish(dead, true));
    assert!(!s.idle());
    assert!(s.finish(next, true));
    assert!(s.idle());
  }

  #[test]
  fn prefetch_does_not_check_a_name_already_etched_earlier_in_the_block() {
    let holder = outpoint(7, 0);
    seed(holder);
    let mut block = reporter_block(holder);
    let mut duplicate = block.txdata[1].0.clone();
    duplicate.input[0].previous_output = outpoint(8, 0);
    let txid = duplicate.compute_txid();
    block.txdata.push((duplicate, txid));

    struct OnlyFirst;
    impl CommitLookup for OnlyFirst {
      async fn taproot_output_height(&self, outpoint: OutPoint) -> Result<Option<u32>> {
        if outpoint.txid == Txid::from_byte_array([9; 32]) {
          Ok(Some(HEIGHT - 10))
        } else {
          Err(anyhow!("lookup for the duplicate must not happen"))
        }
      }
    }

    let commitments = block_on(prefetch_commitments(&OnlyFirst, NETWORK, HEIGHT, &block)).unwrap();
    assert_eq!(commitments, HashMap::from([(1, true)]));
    index_block(NETWORK, HEIGHT, &block, commitments).unwrap();
    assert_eq!(
      crate::index::mem_get_rune_to_rune_id(ETCHED.store()).map(RuneId::load),
      Some(RuneId {
        block: HEIGHT.into(),
        tx: 1
      })
    );
  }

  #[test]
  fn statistics_above_the_tip_mark_a_partly_applied_block() {
    assert!(!crate::index::mem_has_statistics_at(HEIGHT));
    // What upstream left behind when a block failed after its first RPC call.
    crate::index::mem_insert_statistic_runes(HEIGHT, 2);
    assert!(crate::index::mem_has_statistics_at(HEIGHT));
  }
}
