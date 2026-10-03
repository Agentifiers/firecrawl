// Deployments.ai modification: the Google Web Risk provider (whose threat
// lists lived in Redis) was removed, so "normal" mode has no provider and
// every scan resolves through the org's failurePolicy. The provider-backed
// upstream cases were dropped with the provider.
import {
  checkUrl,
  THREAT_PROTECTION_POLICY_DEFAULTS,
  ThreatProtectionPolicy,
  UnsafeDomainBlockedError,
} from "./index";

function policy(
  overrides: Partial<ThreatProtectionPolicy> = {},
): ThreatProtectionPolicy {
  return {
    mode: "normal",
    ...THREAT_PROTECTION_POLICY_DEFAULTS,
    ...overrides,
  };
}

describe("checkUrl", () => {
  it("allows immediately when mode is off, with no provider call", async () => {
    const decision = await checkUrl("example.com", policy({ mode: "off" }), {
      teamId: "team-1",
    });

    expect(decision).toEqual({
      allowed: true,
      rule: "default-allow",
      url: "http://example.com/",
      domain: "example.com",
      providerConsulted: false,
      verdict: null,
      mode: "off",
    });
  });

  it("decides with a local rule without consulting a provider", async () => {
    const decision = await checkUrl(
      "https://cdn.blocked.com/some/page",
      policy({ blacklist: ["blocked.com"] }),
      { teamId: "team-1" },
    );

    expect(decision).toMatchObject({
      allowed: false,
      rule: "blacklist",
      domain: "cdn.blocked.com",
      providerConsulted: false,
      verdict: null,
    });
  });

  it("fails closed without a provider when failurePolicy is closed", async () => {
    const decision = await checkUrl(
      "threat-risky.example",
      policy({ failurePolicy: "closed" }),
      { teamId: "team-1" },
    );

    expect(decision).toEqual({
      allowed: false,
      rule: "provider-failure",
      url: "http://threat-risky.example/",
      domain: "threat-risky.example",
      providerConsulted: false,
      verdict: null,
      mode: "normal",
    });
  });

  it("fails open without a provider when failurePolicy is open", async () => {
    const decision = await checkUrl(
      "threat-risky.example",
      policy({ failurePolicy: "open" }),
      {},
    );

    expect(decision).toMatchObject({
      allowed: true,
      rule: "provider-failure",
      providerConsulted: false,
      verdict: null,
    });
  });
});

describe("UnsafeDomainBlockedError", () => {
  it("carries the URL, domain and decision with the unsafe_domain_blocked code", async () => {
    const decision = await checkUrl(
      "https://bad.example/landing",
      policy({ blacklist: ["bad.example"] }),
      {},
    );
    const error = new UnsafeDomainBlockedError(
      "https://bad.example/landing",
      decision,
    );

    expect(error.code).toBe("unsafe_domain_blocked");
    expect(error.name).toBe("UnsafeDomainBlockedError");
    expect(error.url).toBe("https://bad.example/landing");
    expect(error.domain).toBe("bad.example");
    expect(error.decision).toBe(decision);
    expect(error.message).toContain("threat protection policy");
    expect(error.message).toContain("blacklist");
  });
});
