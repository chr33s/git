import { stores } from "./Memory.ts";
import { storeContract } from "./Store.contract.ts";

storeContract("Memory", { make: () => stores });
