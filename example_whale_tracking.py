#!/usr/bin/env python3
"""
Example standalone script for Birdeye whale tracking
Run this directly without the agent for quick testing
"""

import os
import sys
from birdeye_tracker import WhaleTracker, track_whale, find_pumps, analyze_token, daily_scan


def main():
    # Set your API key (optional but recommended)
    api_key = os.environ.get("HELIUS_API_KEY", None)
    
    if len(sys.argv) < 2:
        print("Usage: python example_whale_tracking.py <command> [args]")
        print("\nAvailable commands:")
        print("  daily          - Run complete daily scan")
        print("  pumps          - Find potential pump tokens")
        print("  track <wallet> - Track specific whale wallet")
        print("  analyze <token> - Analyze token for pump/dump signals")
        print("\nExamples:")
        print("  python example_whale_tracking.py daily")
        print("  python example_whale_tracking.py pumps")
        print("  python example_whale_tracking.py track 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU")
        print("  python example_whale_tracking.py analyze EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    try:
        if command == "daily":
            print("🔍 Running daily whale tracking scan...\n")
            result = daily_scan(api_key)
            print(result)
        
        elif command == "pumps":
            print("💎 Searching for potential pump tokens...\n")
            result = find_pumps(api_key)
            print(result)
        
        elif command == "track":
            if len(sys.argv) < 3:
                print("Error: Please provide wallet address")
                print("Usage: python example_whale_tracking.py track <wallet_address>")
                sys.exit(1)
            
            wallet = sys.argv[2]
            print(f"🐋 Tracking whale wallet: {wallet}\n")
            result = track_whale(wallet, api_key)
            print(result)
        
        elif command == "analyze":
            if len(sys.argv) < 3:
                print("Error: Please provide token address")
                print("Usage: python example_whale_tracking.py analyze <token_address>")
                sys.exit(1)
            
            token = sys.argv[2]
            print(f"📊 Analyzing token: {token}\n")
            result = analyze_token(token, api_key)
            print(result)
        
        else:
            print(f"Unknown command: {command}")
            print("Run without arguments to see usage help")
            sys.exit(1)
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
