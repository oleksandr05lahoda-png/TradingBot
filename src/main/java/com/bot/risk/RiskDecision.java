package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * The engine's answer: either a plan, or a reason there is none. Sealed, so no third outcome can be
 * added quietly — there is no "approved with warnings", which would still place an order.
 */
public sealed interface RiskDecision permits RiskDecision.Approved, RiskDecision.Rejected {

    default boolean isApproved() { return this instanceof Approved; }

    /** The plan, or a failure if this decision was a refusal. Use {@code switch} where possible. */
    default TradePlan planOrThrow() {
        if (this instanceof Approved a) return a.plan();
        Rejected r = (Rejected) this;
        throw new IllegalStateException("trade was rejected: " + r.reason() + " — " + r.detail());
    }

    record Approved(TradePlan plan) implements RiskDecision {
        public Approved {
            Preconditions.notNull(plan, "plan");
        }
    }

    record Rejected(RejectReason reason, String detail) implements RiskDecision {
        public Rejected {
            Preconditions.notNull(reason, "reason");
            Preconditions.notBlank(detail, "detail");
        }

        @Override public String toString() { return "REJECTED " + reason + ": " + detail; }
    }

    static RiskDecision approve(TradePlan plan) { return new Approved(plan); }

    static RiskDecision reject(RejectReason reason, String detail) { return new Rejected(reason, detail); }
}
