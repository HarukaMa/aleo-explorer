
program credits.aleo {

    /// The `committee` mapping contains the active validator set and their corresponding stake.
    // The key represents the address of the validator.
    // The value represents the committee state of the validator.
    mapping committee: address => committee_state;

    // The `committee_state` struct tracks the total stake of the validator, and whether they are open to new stakers.
    struct committee_state {
        // The boolean flag indicating if the validator is open to new stakers.
        is_open: bool,
        // The percentage amount (from 0 to 100, inclusive) of rewards that retained by the validator.
        commission: u8,
    }

    // The `delegated` mapping tracks the total amount of microcredits that are prebonded and bonded to validator addresses.
    // Note: The mapping includes both prebonded and bonded microcredits. However, it does not contain unbonding microcredits.
    // The key represents the address of the validator.
    // The value represents the amount of microcredits bonded to the validator, by the validator and its delegators.
    mapping delegated: address => u64;

    /// The `metadata` mapping stores:
    ///   - The number of members in the committee.
    ///   - The number of delegators.
    // The key represents the index at which the count is stored.
    //    - This address (aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc) stores the number of **members** in the committee.
    //    - This address (aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0) stores the number of **delegators**.
    // The value represents the count.
    mapping metadata: address => u32;

    // The `bonded` mapping represents the amount of microcredits that are currently bonded.
    // The key represents the address of the staker, which includes the validators and their delegators.
    // The value represents the bond state.
    mapping bonded: address => bond_state;

    // The `bond_state` struct tracks the amount of microcredits that are currently bonded to the specified validator.
    struct bond_state {
        // The address of the validator.
        validator: address,
        // The amount of microcredits that are currently bonded to the specified validator.
        microcredits: u64,
    }

    // The `unbonding` mapping contains a set of stakers with their unbonding microcredits and unlock height.
    // The key represents the address of the staker, which includes the validators and their delegators.
    // The value represents the unbond state.
    mapping unbonding: address => unbound_state;

    // The `unbond_state` struct tracks the microcredits that are currently unbonding, along with the unlock height.
    struct unbound_state {
        // The amount of microcredits that are currently unbonding.
        microcredits: u64,
        // The block height at which the unbonding will be complete, and can be claimed.
        height: u32,
    }

    // The `account` mapping is used to store credits publicly.
    // The key represents the address of the owner.
    // The value represents the amount of public microcredits that belong to the specified owner.
    mapping account: address => u64;

    // The `withdraw` mapping contains the staking address and their corresponding withdrawal address.
    // The key represents the staking address of the owner.
    // The value represents the withdrawal address of the owner.
    mapping withdraw: address => address;

    // The `credits` record is used to store credits privately.
    record credits {
        // The address of the owner.
        owner: address,
        // The amount of private microcredits that belong to the specified owner.
        microcredits: u64,
    }

    // This function allows a validator to bond their microcredits to themselves and specify a withdrawal address
    // and commission percentage.
    //
    // The commission percentage is the portion of rewards that the validator may claim for itself.
    // Note: Setting the commission percentage to 100 results in all rewards being retained for the validator.
    //
    // The corresponding functions for 'bond_validator' are 'unbond_public' and 'claim_unbond_public'.
    async transition bond_validator(
        // Input the withdrawal address.
        public withdrawal_address: address,
        // Input the amount of microcredits to bond.
        public microcredits: u64,
        // Input the commission percentage.
        public commission: u8,
    ) -> Future {
        // Ensure the withdrawal address is not the validator address.
        assert(self.signer != withdrawal_address);

        // Enforce the amount is at least 1 credit.
        assert(microcredits >= 1_000_000u64);

        // Ensure the commission percentage does not exceed 100%.
        assert(commission <= 100u8);

        // Bond the specified amount of microcredits to the specified validator.
        return bond_validator_finalize(
            self.signer,
            withdrawal_address,
            microcredits,
            commission,
        );
    }

    async function bond_validator_finalize(
        // Input the validator's address.
        validator: address,
        // Input the withdrawal address.
        withdrawal: address,
        // Input the amount of microcredits to bond.
        microcredits: u64,
        // Input the commission percentage.
        commission: u8,
    ) {
        // Ensure that the withdrawal address is consistent.
        assert(Mapping::get_or_use(withdraw, validator, withdrawal) == withdrawal);

        // Construct the initial committee state.
        // Note: We set the initial 'is_open' state to 'true'.
        let new_committee_state: committee_state = Mapping::get_or_use(committee, validator, committee_state {
            is_open: true,
            commission: commission,
        });

        // Ensure the commission percentage remains unchanged.
        // Note: The commission percentage can only be set when the validator bonds for the first time.
        assert(new_committee_state.commission == commission);


        /* Bonded */

        // Get the bond state for the signer, or default to the initial bond state.
        let old_bond_state: bond_state = Mapping::get_or_use(bonded, validator, bond_state {
            validator: validator,
            microcredits: 0u64,
        });
        // Enforce the validator matches in the bond state.
        assert(old_bond_state.validator == validator);

        // Construct the updated bond state.
        let new_bond_state: bond_state = bond_state {
            validator: validator,
            // Increment the amount of microcredits for the bond state.
            microcredits: old_bond_state.microcredits + microcredits,
        };

        /* Delegated */

        // Include total pre-delegated amount when deciding if validator has enough credits to join committee.
        // First get the current delegated amount, or start with 0 if none delegated.
        let old_delegated: u64 = Mapping::get_or_use(delegated, validator, 0u64);
        // Self-bond amount + existing delegated amount = total bond.
        let new_delegated: u64 = old_delegated + microcredits;
        // Enforce the amount is at least 10 million credits.
        assert(new_delegated >= 10_000_000_000_000u64);

        /* Account */

        // Get the balance of the signer.
        // If the account does not exist, this finalize scope will fail.
        let old_balance: u64 = Mapping::get(account, validator);
        // Decrement the balance of the signer.
        let new_balance: u64 = old_balance - microcredits;

        /* Writes */

        if (Mapping::contains(committee, validator)) {
            // Set the withdrawal address.
            // Note: This operation is only necessary on the first time for a validator entry, in order to initialize the value.
            Mapping::set(withdraw, validator, withdrawal);

            // Ensure that the initial bond is at least 100 credits.
            assert(microcredits >= 100_000_000u64);

            // Get the committee size.
            let old_committee_size: u32 = Mapping::get_or_use(metadata, aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc, 0u32);
            // Increment the committee size by one.
            let new_committee_size: u32 = old_committee_size + 1u32;
            // Set the new committee size.
            Mapping::set(metadata, aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc, new_committee_size);

            // Ensure the validator currently is not unbonding.
            assert(Mapping::contains(unbonding, validator) == false);
        }

        // Update the committee state of the specified validator.
        Mapping::set(committee, validator, new_committee_state);
        // Update the delegated amount.
        Mapping::set(delegated, validator, new_delegated);
        // Update the bond state for the signer.
        Mapping::set(bonded, validator, new_bond_state);
        // Update the balance of the signer.
        Mapping::set(account, validator, new_balance);

    }

    // This function allows any delegator to bond their microcredits to a validator and specify a withdrawal address.
    //
    // The declared validator is **not** required to be in the committee yet, however it may **not** be unbonding.
    // This function enforces a minimum of 10,000 microcredits for each delegator.
    // The maximum number of delegators allowed in the committee is 100,000 delegator addresses.
    //
    // The corresponding functions for 'bond_public' are 'bond_validator', 'unbond_public', and 'claim_unbond_public'.
    async transition bond_public(
        public validator: address,
        public withdrawal: address,
        public microcredits: u64,
    ) -> Future {
        // Enforce the amount is at least 1 credit.
        assert(microcredits >= 1_000_000u64);

        assert(self.caller != validator);

        // Bond the specified amount of microcredits to the specified validator.
        return bond_public_finalize(
            self.caller,
            validator,
            withdrawal,
            microcredits,
        );
    }

    async function bond_public_finalize(
        // Input the staker's address.
        staker: address,
        // Input the validator's address.
        validator: address,
        // Input the withdrawal address.
        withdrawal: address,
        // Input the amount of microcredits to bond.
        microcredits: u64,
    ) {
        // Ensure that the withdrawal address is consistent.
        assert(Mapping::get_or_use(withdraw, staker, withdrawal) == withdrawal);

        if (Mapping::contains(bonded, staker)) {
            // Set the withdrawal address.
            // Note: This operation is only necessary on the first time for a staker entry, in order to initialize the value.
            Mapping::set(withdraw, staker, withdrawal);

            // Ensure that the validator is open to new stakers. By default, `is_open` is set to `true`.
            let default_committee_state: committee_state = committee_state {
                is_open: true,
                commission: 0u8,
            };
            assert(Mapping::get_or_use(committee, validator, default_committee_state).is_open == true);

            // Get the number of delegators.
            let old_delegators: u32 = Mapping::get_or_use(metadata, aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0, 0u32);
            // Increment the number of bonded delegators by one.
            let new_delegators: u32 = old_delegators + 1u32;
            // Enforce that the number of delegators is less than or equal to 100_000.
            assert(new_delegators <= 100_000u32);
            // Set the new number of delegators.
            Mapping::set(metadata, aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0, new_delegators);
        }

        /* Bonded */

        // TODO
    )

}