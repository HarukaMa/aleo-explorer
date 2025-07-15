import os
from enum import auto, IntEnum
from io import BytesIO, SEEK_CUR
from types import GenericAlias
from typing import TypeVar, Generic, Type, Any, cast

import aleo_explorer_rust
import rocksdbpy

from aleo_types import Serializable, Tuple, Vec, tp_cache, is_serializable, Option, FixedSize
from aleo_types.basic import IntEnumu16, Unit, u16, u32, u64, bool_, u8, Int
from aleo_types.traits import Sized
from aleo_types.vm_basic import BlockHash, StateRoot, Field, TransactionID, Group, TransitionID
from aleo_types.vm_block import AcceptedDeploy, AcceptedExecute, AcceptedExecuteType, Block, BlockHeader, Authority, \
    ConfirmedTxType, ConstantTransitionInput, DeployTransaction, Deployment, ExecuteTransaction, Execution, \
    FeeTransaction, PublicTransitionInput, \
    Ratifications, RejectedDeployType, \
    RejectedExecuteType, Solutions, \
    SolutionID, ConfirmedTransaction, \
    FinalizeOperation, Rejected, Transactions, AcceptedDeployType, RejectedDeploy, RejectedExecute, \
    ProgramOwner, Program, TransitionInput, TransitionOutput, VerifyingKey, Certificate, Proof, Plaintext, Ciphertext, \
    Future, Record, \
    TransactionType, \
    Transition, PrivateTransitionInput, RecordTransitionInput, ExternalRecordTransitionInput, ConstantTransitionOutput, \
    PublicTransitionOutput, PrivateTransitionOutput, RecordTransitionOutput, ExternalRecordTransitionOutput, \
    FutureTransitionOutput, Fee
from aleo_types.vm_instruction import Identifier, ProgramID


class DataID(IntEnumu16):
    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: list[int]):
        return count

    # BFT
    BFTTransmissionsMap = auto()
    BFTAbortedTransmissionIDsMap = auto()
    # Block
    BlockStateRootMap = auto()
    BlockReverseStateRootMap = auto()
    BlockIDMap = auto()
    BlockReverseIDMap = auto()
    BlockHeaderMap = auto()
    BlockAuthorityMap = auto()
    BlockCertificateMap = auto()
    BlockRatificationsMap = auto()
    BlockSolutionsMap = auto()
    BlockSolutionIDsMap = auto()
    BlockAbortedSolutionIDsMap = auto()
    BlockAbortedSolutionHeightsMap = auto()
    BlockTransactionsMap = auto()
    BlockAbortedTransactionIDsMap = auto()
    BlockRejectedOrAbortedTransactionIDMap = auto()
    BlockConfirmedTransactionsMap = auto()
    BlockRejectedDeploymentOrExecutionMap = auto()
    # Committee
    CurrentRoundMap = auto()
    RoundToHeightMap = auto()
    CommitteeMap = auto()
    # Deployment
    DeploymentIDMap = auto()
    DeploymentEditionMap = auto()
    DeploymentReverseIDMap = auto()
    DeploymentOwnerMap = auto()
    DeploymentProgramMap = auto()
    DeploymentVerifyingKeyMap = auto()
    DeploymentCertificateMap = auto()
    # Execution
    ExecutionIDMap = auto()
    ExecutionReverseIDMap = auto()
    ExecutionInclusionMap = auto()
    # Fee
    FeeFeeMap = auto()
    FeeReverseFeeMap = auto()
    # Input
    InputIDMap = auto()
    InputReverseIDMap = auto()
    InputConstantMap = auto()
    InputPublicMap = auto()
    InputPrivateMap = auto()
    InputRecordMap = auto()
    InputRecordTagMap = auto()
    InputExternalRecordMap = auto()
    # Output
    OutputIDMap = auto()
    OutputReverseIDMap = auto()
    OutputConstantMap = auto()
    OutputPublicMap = auto()
    OutputPrivateMap = auto()
    OutputRecordMap = auto()
    OutputRecordNonceMap = auto()
    OutputExternalRecordMap = auto()
    OutputFutureMap = auto()
    # Transaction
    TransactionIDMap = auto()
    # Transition
    TransitionLocatorMap = auto()
    TransitionTPKMap = auto()
    TransitionReverseTPKMap = auto()
    TransitionTCMMap = auto()
    TransitionReverseTCMMap = auto()
    TransitionSCMMap = auto()
    # Program
    ProgramIDMap = auto()
    KeyValueMap = auto()

    # For Backwards Compatibility with Existing Databases
    OutputRecordSenderMap = auto()
    # Track edition based on transaction ID
    IDEditionMap = auto()


K = TypeVar("K", bound=Serializable)
V = TypeVar("V", bound=Serializable)
L = TypeVar('L', bound=Int | FixedSize)

def serialize(kt: Type[K] | GenericAlias, key: K) -> bytes:
    if isinstance(kt, GenericAlias):
        if issubclass(kt.__origin__, Tuple):
            types = cast(tuple[Type[K], ...], kt.types)
            res = b""
            for t, v in zip(types, key):
                res += serialize(t, v)
            return res
        elif issubclass(kt.__origin__, Vec):
            raise NotImplementedError
        elif issubclass(kt.__origin__, Option):
            raise NotImplementedError

    res = key.dump()
    if not(
        (isinstance(kt, GenericAlias)
         and issubclass(kt.__origin__, (Vec, Option, Tuple))
        ) or isinstance(kt, Sized)
        or (
            not isinstance(kt, GenericAlias)
            and (issubclass(kt, IntEnum)
                 or issubclass(kt, Identifier)
            )
        )
    ):
        res = len(res).to_bytes(8, "little") + res
    return res

def deserialize(vt: Type[V] | GenericAlias, buf: BytesIO) -> V:
    if isinstance(vt, GenericAlias):
        if issubclass(vt.__origin__, Tuple):
            types = cast(tuple[Type[V], ...], vt.types)
            values: list[V] = []
            for t in types:
                values.append(deserialize(t, buf))
            return vt(values)
        elif issubclass(vt.__origin__, Vec):
            types = cast(tuple[Type[V], Type[L]], vt.types)
            if isinstance(types[1], FixedSize):
                size = types[1]
            else:
                size = types[1].load(buf)
            return vt(list(deserialize(types[0], buf) for _ in range(size)))

        elif issubclass(vt.__origin__, Option):
            types = cast(Type[V], vt.types)
            is_some = bool_.load(buf)
            if is_some:
                value = deserialize(types, buf)
            else:
                value = None
            return vt(value)

    if not(
        (isinstance(vt, GenericAlias)
         and issubclass(vt.__origin__, (Vec, Option, Tuple))
        ) or isinstance(vt, Sized)
        or (
            not isinstance(vt, GenericAlias)
            and (issubclass(vt, IntEnum)
                 or issubclass(vt, Identifier)
            )
        )
    ):
        buf.seek(8, SEEK_CUR)
    vt = cast(Type[V], vt)
    return vt.load(buf)

class DataMap(Generic[K, V]):
    key_type: Type[K]
    value_type: Type[V]

    def __init__(self, map_id: DataID):
        self.map_id = map_id

    @tp_cache
    def __class_getitem__(cls, key: Any | tuple[Any, ...]) -> GenericAlias:
        if not isinstance(key, tuple):
            raise TypeError("expected key and value type")
        if len(key) != 2:
            raise TypeError("expected key and value type")
        kt, vt = key
        if not is_serializable(kt):
            raise TypeError(f"expected Serializable type, got {kt}")
        if not is_serializable(vt):
            raise TypeError(f"expected Serializable type, got {vt}")
        class_name = f"DataMap[{kt.__name__}, {vt.__name__}]"
        param_type = type(
            class_name,
            (DataMap,),
            {"key_type": kt, "value_type": vt},
        )
        return GenericAlias(param_type, key)

    def read(self, rdb: rocksdbpy.RocksDB, key: K):
        from node import Network

        raw = rdb.get(Network.network_id.dump() + self.map_id.dump() + serialize(self.key_type, key))
        if raw is None:
            return None
        buffer = BytesIO(raw)

        return deserialize(self.value_type, buffer)


#
#     type StateRootMap = DataMap<u32, N::StateRoot>;
#     type ReverseStateRootMap = DataMap<N::StateRoot, u32>;
#     type IDMap = DataMap<u32, N::BlockHash>;
#     type ReverseIDMap = DataMap<N::BlockHash, u32>;
#     type HeaderMap = DataMap<N::BlockHash, Header<N>>;
#     type AuthorityMap = DataMap<N::BlockHash, Authority<N>>;
#     type CertificateMap = DataMap<Field<N>, (u32, u64)>;
#     type RatificationsMap = DataMap<N::BlockHash, Ratifications<N>>;
#     type SolutionsMap = DataMap<N::BlockHash, Solutions<N>>;
#     type SolutionIDsMap = DataMap<SolutionID<N>, u32>;
#     type AbortedSolutionIDsMap = DataMap<N::BlockHash, Vec<SolutionID<N>>>;
#     type AbortedSolutionHeightsMap = DataMap<SolutionID<N>, u32>;
#     type TransactionsMap = DataMap<N::BlockHash, Vec<N::TransactionID>>;
#     type AbortedTransactionIDsMap = DataMap<N::BlockHash, Vec<N::TransactionID>>;
#     type RejectedOrAbortedTransactionIDMap = DataMap<N::TransactionID, N::BlockHash>;
#     type ConfirmedTransactionsMap = DataMap<N::TransactionID, (N::BlockHash, ConfirmedTxType<N>, Vec<FinalizeOperation<N>>)>;
#     type RejectedDeploymentOrExecutionMap = DataMap<Field<N>, Rejected<N>>;
#

StateRootMap = DataMap[u32, StateRoot](DataID.BlockStateRootMap)
ReverseStateRootMap = DataMap[StateRoot, u32](DataID.BlockReverseStateRootMap)
IDMap = DataMap[u32, BlockHash](DataID.BlockIDMap)
ReverseIDMap = DataMap[BlockHash, u32](DataID.BlockReverseIDMap)
HeaderMap = DataMap[BlockHash, BlockHeader](DataID.BlockHeaderMap)
AuthorityMap = DataMap[BlockHash, Authority](DataID.BlockAuthorityMap)
CertificateMap = DataMap[Field, Tuple[u32, u64]](DataID.BlockCertificateMap)
RatificationsMap = DataMap[BlockHash, Ratifications](DataID.BlockRatificationsMap)
SolutionsMap = DataMap[BlockHash, Solutions](DataID.BlockSolutionsMap)
SolutionIDsMap = DataMap[SolutionID, u32](DataID.BlockSolutionIDsMap)
AbortedSolutionIDsMap = DataMap[BlockHash, Vec[SolutionID, u64]](DataID.BlockAbortedSolutionIDsMap)
AbortedSolutionHeightsMap = DataMap[SolutionID, u32](DataID.BlockAbortedSolutionHeightsMap)
TransactionsMap = DataMap[BlockHash, Vec[TransactionID, u64]](DataID.BlockTransactionsMap)
AbortedTransactionIDsMap = DataMap[BlockHash, Vec[TransactionID, u64]](DataID.BlockAbortedTransactionIDsMap)
RejectedOrAbortedTransactionIDMap = DataMap[TransactionID, BlockHash](DataID.BlockRejectedOrAbortedTransactionIDMap)
ConfirmedTransactionsMap = DataMap[TransactionID, Tuple[BlockHash, ConfirmedTxType, Vec[FinalizeOperation, u64]]](
    DataID.BlockConfirmedTransactionsMap)
RejectedDeploymentOrExecutionMap = DataMap[Field, Rejected](DataID.BlockRejectedDeploymentOrExecutionMap)

# type IDMap = DataMap<N::TransactionID, ProgramID<N>>;
# type EditionMap = DataMap<ProgramID<N>, u16>;
# type ReverseIDMap = DataMap<(ProgramID<N>, u16), N::TransactionID>;
# type OwnerMap = DataMap<(ProgramID<N>, u16), ProgramOwner<N>>;
# type ProgramMap = DataMap<(ProgramID<N>, u16), Program<N>>;
# type VerifyingKeyMap = DataMap<(ProgramID<N>, Identifier<N>, u16), VerifyingKey<N>>;
# type CertificateMap = DataMap<(ProgramID<N>, Identifier<N>, u16), Certificate<N>>;
# type FeeStorage = FeeDB<N>;

DeploymentIDMap = DataMap[TransactionID, ProgramID](DataID.DeploymentIDMap)
DeploymentEditionMap = DataMap[ProgramID, u16](DataID.DeploymentEditionMap)
DeploymentReverseIDMap = DataMap[Tuple[ProgramID, u16], TransactionID](DataID.DeploymentReverseIDMap)
DeploymentOwnerMap = DataMap[Tuple[ProgramID, u16], ProgramOwner](DataID.DeploymentOwnerMap)
DeploymentProgramMap = DataMap[Tuple[ProgramID, u16], Program](DataID.DeploymentProgramMap)
DeploymentVerifyingKeyMap = DataMap[Tuple[ProgramID, Identifier, u16], VerifyingKey](DataID.DeploymentVerifyingKeyMap)
DeploymentCertificateMap = DataMap[Tuple[ProgramID, Identifier, u16], Certificate](DataID.DeploymentCertificateMap)

# type IDMap = DataMap<N::TransactionID, (Vec<N::TransitionID>, bool)>;
# type ReverseIDMap = DataMap<N::TransitionID, N::TransactionID>;
# type InclusionMap = DataMap<N::TransactionID, (N::StateRoot, Option<Proof<N>>)>;
# type FeeStorage = FeeDB<N>;

ExecutionIDMap = DataMap[TransactionID, Tuple[Vec[TransitionID, u64], bool_]](DataID.ExecutionIDMap)
ExecutionReverseIDMap = DataMap[TransitionID, TransactionID](DataID.ExecutionReverseIDMap)
ExecutionInclusionMap = DataMap[TransactionID, Tuple[StateRoot, Option[Proof]]](DataID.ExecutionInclusionMap)

# type FeeMap = DataMap<N::TransactionID, (N::TransitionID, N::StateRoot, Option<Proof<N>>)>;
# type ReverseFeeMap = DataMap<N::TransitionID, N::TransactionID>;
# type TransitionStorage = TransitionDB<N>;

FeeMap = DataMap[TransactionID, Tuple[TransitionID, StateRoot, Option[Proof]]](DataID.FeeFeeMap)
ReverseFeeMap = DataMap[TransitionID, TransactionID](DataID.FeeReverseFeeMap)

# type IDMap = DataMap<N::TransitionID, Vec<Field<N>>>;
# type ReverseIDMap = DataMap<Field<N>, N::TransitionID>;
# type ConstantMap = DataMap<Field<N>, Option<Plaintext<N>>>;
# type PublicMap = DataMap<Field<N>, Option<Plaintext<N>>>;
# type PrivateMap = DataMap<Field<N>, Option<Ciphertext<N>>>;
# type RecordMap = DataMap<Field<N>, Field<N>>;
# type RecordTagMap = DataMap<Field<N>, Field<N>>;
# type ExternalRecordMap = DataMap<Field<N>, ()>;

InputIDMap = DataMap[TransitionID, Vec[Field, u64]](DataID.InputIDMap)
InputReverseIDMap = DataMap[Field, TransitionID](DataID.InputReverseIDMap)
InputConstantMap = DataMap[Field, Option[Plaintext]](DataID.InputConstantMap)
InputPublicMap = DataMap[Field, Option[Plaintext]](DataID.InputPublicMap)
InputPrivateMap = DataMap[Field, Option[Ciphertext]](DataID.InputPrivateMap)
InputRecordMap = DataMap[Field, Field](DataID.InputRecordMap)
InputRecordTagMap = DataMap[Field, Field](DataID.InputRecordTagMap)
InputExternalRecordMap = DataMap[Field, Unit](DataID.InputExternalRecordMap)

# type IDMap = DataMap<N::TransitionID, Vec<Field<N>>>;
# type ReverseIDMap = DataMap<Field<N>, N::TransitionID>;
# type ConstantMap = DataMap<Field<N>, Option<Plaintext<N>>>;
# type PublicMap = DataMap<Field<N>, Option<Plaintext<N>>>;
# type PrivateMap = DataMap<Field<N>, Option<Ciphertext<N>>>;
# type RecordMap = DataMap<Field<N>, (Field<N>, Option<Record<N, Ciphertext<N>>>)>;
# type RecordSenderMap = DataMap<Group<N>, Option<Field<N>>>;
# type RecordNonceMap = DataMap<Group<N>, Field<N>>;
# type ExternalRecordMap = DataMap<Field<N>, ()>;
# type FutureMap = DataMap<Field<N>, Option<Future<N>>>;

OutputIDMap = DataMap[TransitionID, Vec[Field, u64]](DataID.OutputIDMap)
OutputReverseIDMap = DataMap[Field, TransitionID](DataID.OutputReverseIDMap)
OutputConstantMap = DataMap[Field, Option[Plaintext]](DataID.OutputConstantMap)
OutputPublicMap = DataMap[Field, Option[Plaintext]](DataID.OutputPublicMap)
OutputPrivateMap = DataMap[Field, Option[Ciphertext]](DataID.OutputPrivateMap)
OutputRecordMap = DataMap[Field, Tuple[Field, Option[Record[Ciphertext]]]](DataID.OutputRecordMap)
OutputRecordSenderMap = DataMap[Group, Option[Field]](DataID.OutputRecordSenderMap)
OutputRecordNonceMap = DataMap[Group, Field](DataID.OutputRecordNonceMap)
OutputExternalRecordMap = DataMap[Field, Unit](DataID.OutputExternalRecordMap)
OutputFutureMap = DataMap[Field, Option[Future]](DataID.OutputFutureMap)

# type IDMap = DataMap<N::TransactionID, TransactionType>;

TransactionIDMap = DataMap[TransactionID, TransactionType](DataID.TransactionIDMap)

# type LocatorMap = DataMap<N::TransitionID, (ProgramID<N>, Identifier<N>)>;
# type InputStorage = InputDB<N>;
# type OutputStorage = OutputDB<N>;
# type TPKMap = DataMap<N::TransitionID, Group<N>>;
# type ReverseTPKMap = DataMap<Group<N>, N::TransitionID>;
# type TCMMap = DataMap<N::TransitionID, Field<N>>;
# type ReverseTCMMap = DataMap<Field<N>, N::TransitionID>;
# type SCMMap = DataMap<N::TransitionID, Field<N>>;

TransitionLocatorMap = DataMap[TransitionID, Tuple[ProgramID, Identifier]](DataID.TransitionLocatorMap)
TransitionTPKMap = DataMap[TransitionID, Group](DataID.TransitionTPKMap)
TransitionReverseTPKMap = DataMap[Group, TransitionID](DataID.TransitionReverseTPKMap)
TransitionTCMMap = DataMap[TransitionID, Field](DataID.TransitionTCMMap)
TransitionReverseTCMMap = DataMap[Field, TransitionID](DataID.TransitionReverseTCMMap)
TransitionSCMMap = DataMap[TransitionID, Field](DataID.TransitionSCMMap)


class RocksDB:
    def __init__(self, path: str):
        from node import Network
        self.path = path
        opts = rocksdbpy.Option()
        opts.create_if_missing(False)
        secondary_dir_name = f"ledger-{Network.network_id}-secondary"
        os.makedirs(secondary_dir_name, exist_ok=True)
        self.rdb = rocksdbpy.open_as_secondary(self.path, secondary_dir_name, opts)

    def catch_up(self):
        self.rdb.try_catch_up_with_primary()

    def get_block(self, block_height: int):
        block_hash = self.get_block_hash_from_height(block_height)
        if block_hash is None:
            return None
        header = self.get_block_header(block_hash)
        if header is None:
            raise ValueError(f"missing header for block {block_hash} in rdb")
        previous_hash = self.get_previous_block_hash(block_height)
        if previous_hash is None:
            raise ValueError(f"missing previous block hash for block {block_height} in rdb")
        authority = AuthorityMap.read(self.rdb, block_hash)
        if authority is None:
            raise ValueError(f"missing authority for block {block_hash} in rdb")
        ratifications = RatificationsMap.read(self.rdb, block_hash)
        if ratifications is None:
            raise ValueError(f"missing ratifications for block {block_hash} in rdb")
        solutions = SolutionsMap.read(self.rdb, block_hash)
        if solutions is None:
            raise ValueError(f"missing solutions for block {block_hash} in rdb")
        aborted_solution_ids = AbortedSolutionIDsMap.read(self.rdb, block_hash)
        if aborted_solution_ids is None:
            raise ValueError(f"missing aborted solution ids for block {block_hash} in rdb")
        transactions = self.get_block_transactions(block_hash)
        if transactions is None:
            raise ValueError(f"missing transactions for block {block_hash} in rdb")
        aborted_transaction_ids = AbortedTransactionIDsMap.read(self.rdb, block_hash)
        if aborted_transaction_ids is None:
            raise ValueError(f"missing aborted transaction ids for block {block_hash} in rdb")
        return Block(
            block_hash=block_hash,
            previous_hash=previous_hash,
            header=header,
            authority=authority,
            ratifications=ratifications,
            solutions=solutions,
            aborted_solution_ids=Vec[SolutionID, u32](aborted_solution_ids),
            transactions=transactions,
            aborted_transaction_ids=Vec[TransactionID, u32](aborted_transaction_ids),
        )

    def get_previous_block_hash(self, block_height: int):
        if block_height == 0:
            return BlockHash(b"\x00" * 32)
        return self.get_block_hash_from_height(block_height - 1)

    def get_block_hash_from_height(self, block_height: int):
        return IDMap.read(self.rdb, u32(block_height))

    def get_block_header(self, block_hash: BlockHash):
        return HeaderMap.read(self.rdb, block_hash)

    def get_block_authority(self, block_hash: BlockHash):
        return AuthorityMap.read(self.rdb, block_hash)

    def get_block_ratifications(self, block_hash: BlockHash):
        return RatificationsMap.read(self.rdb, block_hash)

    def get_block_solutions(self, block_hash: BlockHash):
        return SolutionsMap.read(self.rdb, block_hash)

    def get_block_aborted_solution_ids(self, block_hash: BlockHash):
        return AbortedSolutionIDsMap.read(self.rdb, block_hash)

    def get_block_transactions(self, block_hash: BlockHash):
        transaction_ids = TransactionsMap.read(self.rdb, block_hash)
        if transaction_ids is None:
            return None
        transactions: list[ConfirmedTransaction] = []
        for txid in transaction_ids:
            transactions.append(self.get_confirmed_transaction(txid))
        return Transactions(transactions=Vec[ConfirmedTransaction, u32](transactions))

    def get_confirmed_transaction(self, transaction_id: TransactionID):
        transaction = self.get_transaction(transaction_id)
        if transaction is None:
            raise ValueError("missing transaction in rdb")
        data = ConfirmedTransactionsMap.read(self.rdb, transaction_id)
        if data is None:
            raise ValueError("missing confirmed transaction in rdb")
        _, tx_type, finalize_ops = data
        finalize_ops = Vec[FinalizeOperation, u16](finalize_ops)
        if isinstance(tx_type, AcceptedDeployType):
            return AcceptedDeploy(index=tx_type.index, transaction=transaction, finalize=finalize_ops)
        elif isinstance(tx_type, AcceptedExecuteType):
            return AcceptedExecute(index=tx_type.index, transaction=transaction, finalize=finalize_ops)
        elif isinstance(tx_type, RejectedDeployType):
            return RejectedDeploy(index=tx_type.index, transaction=transaction, rejected=tx_type.rejected,
                                  finalize=finalize_ops)
        elif isinstance(tx_type, RejectedExecuteType):
            return RejectedExecute(index=tx_type.index, transaction=transaction, rejected=tx_type.rejected,
                                   finalize=finalize_ops)
        else:
            raise ValueError("unknown transaction type")

    def get_transaction(self, transaction_id: TransactionID):
        block_hash = RejectedOrAbortedTransactionIDMap.read(self.rdb, transaction_id)
        if block_hash is None:
            return self.tx_get_transaction(transaction_id)
        transactions = self.get_block_transactions(block_hash)
        if transactions is None:
            raise ValueError(f"missing transactions for block {block_hash} in rdb")
        for tx in transactions:
            if isinstance(tx, (RejectedDeploy, RejectedExecute)):
                if aleo_explorer_rust.rejected_tx_original_id(tx.dump()) == str(transaction_id):
                    return tx.transaction
        aborted_ids = self.get_block_aborted_transaction_ids(block_hash)
        if aborted_ids is None or transaction_id not in aborted_ids:
            raise ValueError("missing transaction in rdb")
        else:
            raise ValueError(f"transaction {transaction_id} aborted in block {block_hash}")

    def get_block_aborted_transaction_ids(self, block_hash: BlockHash):
        return AbortedTransactionIDsMap.read(self.rdb, block_hash)

    def tx_get_transaction(self, transaction_id: TransactionID):
        transaction_type = TransactionIDMap.read(self.rdb, transaction_id)
        if transaction_type is None:
            return None
        if transaction_type == TransactionType.Deploy:
            return self.deploy_get_transaction(transaction_id)
        elif transaction_type == TransactionType.Execute:
            return self.execute_get_transaction(transaction_id)
        elif transaction_type == TransactionType.Fee:
            fee = self.fee_get_fee(transaction_id)
            if fee is None:
                raise ValueError(f"missing fee for transaction {transaction_id}")
            return FeeTransaction(id_=transaction_id, fee=fee)
        else:
            raise ValueError(f"unknown transaction type {transaction_type}")

    def deploy_get_transaction(self, transaction_id: TransactionID):
        deployment = self.deploy_get_deployment(transaction_id)
        if deployment is None:
            return None
        fee = self.fee_get_fee(transaction_id)
        if fee is None:
            raise ValueError(f"missing fee for transaction {transaction_id}")
        owner = self.deploy_get_owner(deployment.program.id)
        if owner is None:
            raise ValueError(f"missing owner for transaction {transaction_id}")
        return DeployTransaction(id_=transaction_id, deployment=deployment, fee=fee, owner=owner)

    def deploy_get_deployment(self, transaction_id: TransactionID):
        program_id = DeploymentIDMap.read(self.rdb, transaction_id)
        if program_id is None:
            return None
        edition = self.deploy_get_edition(program_id)
        if edition is None:
            raise ValueError(f"missing edition for program {program_id}")
        program = DeploymentProgramMap.read(self.rdb, Tuple[ProgramID, u16]((program_id, edition)))
        if program is None:
            raise ValueError(f"missing program {program_id} edition {edition}")
        verifying_keys: list[Tuple[Identifier, VerifyingKey, Certificate]] = []
        for function in program.functions.keys():
            key = DeploymentVerifyingKeyMap.read(self.rdb,
                                                 Tuple[ProgramID, Identifier, u16]((program_id, function, edition)))
            if key is None:
                raise ValueError(f"missing key for program {program_id} function {function} edition {edition}")
            cert = DeploymentCertificateMap.read(self.rdb,
                                                 Tuple[ProgramID, Identifier, u16]((program_id, function, edition)))
            if cert is None:
                raise ValueError(f"missing certificate for program {program_id} function {function} edition {edition}")
            verifying_keys.append(Tuple[Identifier, VerifyingKey, Certificate]((function, key, cert)))
        return Deployment(edition=edition, program=program,
                          verifying_keys=Vec[Tuple[Identifier, VerifyingKey, Certificate], u16](verifying_keys))

    def deploy_get_edition(self, program_id: ProgramID):
        if program_id == "credits.aleo":
            return None
        return DeploymentEditionMap.read(self.rdb, program_id)

    def deploy_get_owner(self, program_id: ProgramID):
        if program_id == "credits.aleo":
            return None
        edition = self.deploy_get_edition(program_id)
        if edition is None:
            return None
        owner = DeploymentOwnerMap.read(self.rdb, Tuple[ProgramID, u16]((program_id, edition)))
        if owner is None:
            raise ValueError(f"missing owner for program {program_id} edition {edition}")
        return owner

    def fee_get_fee(self, transaction_id: TransactionID):
        data = FeeMap.read(self.rdb, transaction_id)
        if data is None:
            return None
        transition_id, state_root, proof = data
        transition = self.ts_get_transition(transition_id)
        if transition is None:
            raise ValueError(f"missing transition for fee {transaction_id}")
        return Fee(transition=transition, global_state_root=state_root, proof=proof)

    def ts_get_transition(self, transition_id: TransitionID):
        data = TransitionLocatorMap.read(self.rdb, transition_id)
        if data is None:
            return None
        program_id, function = data
        inputs = self.input_get_inputs(transition_id)
        outputs = self.output_get_outputs(transition_id)
        tpk = TransitionTPKMap.read(self.rdb, transition_id)
        tcm = TransitionTCMMap.read(self.rdb, transition_id)
        scm = TransitionSCMMap.read(self.rdb, transition_id)
        if tpk is None or tcm is None or scm is None:
            raise ValueError(f"missing data for transition {transition_id}")
        return Transition(
            id_=transition_id,
            program_id=program_id,
            function_name=function,
            inputs=Vec[TransitionInput, u8](inputs),
            outputs=Vec[TransitionOutput, u8](outputs),
            tpk=tpk,
            tcm=tcm,
            scm=scm
        )

    def input_get_inputs(self, transition_id: TransitionID):
        data = InputIDMap.read(self.rdb, transition_id)
        if data is None:
            return cast(list[TransitionInput], [])
        inputs: list[TransitionInput] = []
        for input_id in data:
            constant = InputConstantMap.read(self.rdb, input_id)
            public = InputPublicMap.read(self.rdb, input_id)
            private = InputPrivateMap.read(self.rdb, input_id)
            record = InputRecordMap.read(self.rdb, input_id)
            external_record = InputExternalRecordMap.read(self.rdb, input_id)
            if constant is not None:
                inputs.append(ConstantTransitionInput(plaintext_hash=input_id, plaintext=constant))
            elif public is not None:
                inputs.append(PublicTransitionInput(plaintext_hash=input_id, plaintext=public))
            elif private is not None:
                inputs.append(PrivateTransitionInput(ciphertext_hash=input_id, ciphertext=private))
            elif record is not None:
                inputs.append(RecordTransitionInput(serial_number=input_id, tag=record))
            elif external_record is not None:
                inputs.append(ExternalRecordTransitionInput(input_commitment=input_id))
            else:
                raise ValueError(f"missing input data for {input_id}")
        return inputs

    def output_get_outputs(self, transition_id: TransitionID):
        data = OutputIDMap.read(self.rdb, transition_id)
        if data is None:
            return cast(list[TransitionOutput], [])
        outputs: list[TransitionOutput] = []
        for output_id in data:
            constant = OutputConstantMap.read(self.rdb, output_id)
            public = OutputPublicMap.read(self.rdb, output_id)
            private = OutputPrivateMap.read(self.rdb, output_id)
            record = OutputRecordMap.read(self.rdb, output_id)
            external_record = OutputExternalRecordMap.read(self.rdb, output_id)
            future = OutputFutureMap.read(self.rdb, output_id)
            if constant is not None:
                outputs.append(ConstantTransitionOutput(plaintext_hash=output_id, plaintext=constant))
            elif public is not None:
                outputs.append(PublicTransitionOutput(plaintext_hash=output_id, plaintext=public))
            elif private is not None:
                outputs.append(PrivateTransitionOutput(ciphertext_hash=output_id, ciphertext=private))
            elif record is not None:
                if record[1].value is not None:
                    nonce = record[1].value.nonce
                    sender_ciphertext = OutputRecordSenderMap.read(self.rdb, nonce)
                    if sender_ciphertext is None:
                        sender_ciphertext = Option[Field](None)
                else:
                    sender_ciphertext = Option[Field](None)
                outputs.append(RecordTransitionOutput(
                    commitment=output_id,
                    checksum=record[0],
                    record_ciphertext=record[1],
                    sender_ciphertext=sender_ciphertext
                ))
            elif external_record is not None:
                outputs.append(ExternalRecordTransitionOutput(commitment=output_id))
            elif future is not None:
                outputs.append(FutureTransitionOutput(future_hash=output_id, future=future))
            else:
                raise ValueError(f"missing output data for {output_id}")
        return outputs

    def execute_get_transaction(self, transaction_id: TransactionID):
        data = ExecutionIDMap.read(self.rdb, transaction_id)
        if data is None:
            return None
        transition_ids, has_fee = data
        data = ExecutionInclusionMap.read(self.rdb, transaction_id)
        if data is None:
            raise ValueError(f"missing inclusion for transaction {transaction_id}")
        global_state_root, proof = data
        transitions: list[Transition] = []
        for transition_id in transition_ids:
            transition = self.ts_get_transition(transition_id)
            if transition is None:
                raise ValueError(f"missing transition {transition_id}")
            transitions.append(transition)
        execution = Execution(
            transitions=Vec[Transition, u8](transitions),
            global_state_root=global_state_root,
            proof=proof
        )
        if has_fee:
            fee = self.fee_get_fee(transaction_id)
            if fee is None:
                raise ValueError(f"missing fee for transaction {transaction_id}")
            return ExecuteTransaction(id_=transaction_id, execution=execution, fee=Option[Fee](fee))
        else:
            return ExecuteTransaction(id_=transaction_id, execution=execution, fee=Option[Fee](None))