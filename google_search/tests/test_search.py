"""
TEST SCRIPT - Run this to test your setup
"""
from config import AppConfig
from main import GoogleSearchTool, setup_logging

# Setup logging
setup_logging(log_level="INFO")

print("="*70)
print(" GOOGLE SEARCH TOOL - TEST SCRIPT")
print("="*70)

try:
    # Load config
    print("\n1. Loading config.yaml...")
    config = AppConfig.from_file("config.yaml")
    print(f"   ✅ Config loaded: {len(config.api.api_keys)} API keys found")
    
    # Validate config
    print("\n2. Validating configuration...")
    config.validate()
    print("   ✅ Configuration valid")
    
    # Initialize tool
    print("\n3. Initializing search tool...")
    tool = GoogleSearchTool(config)
    print("   ✅ Tool initialized")
    
    # Perform test search
    print("\n4. Performing test search (5 results)...")
    print("   Query: 'climate adaptation'")
    print("   This may take 30-60 seconds...")
    
    csv_path = tool.search(
        query="climate adaptation",
        output_name="test_results",
        max_results=5  # Small test
    )
    
    print(f"\n✅ SUCCESS! Results saved to:")
    print(f"   {csv_path}")
    print(f"\n   Check the file to see your search results!")
    
    print("\n" + "="*70)
    print(" TEST COMPLETE - YOUR SETUP WORKS!")
    print("="*70)
    print("\nNext steps:")
    print("- Modify config.yaml to change search settings")
    print("- Run larger searches with date ranges")
    print("- Check data/_results/ folder for outputs")
    
except FileNotFoundError as e:
    print(f"\n❌ ERROR: {e}")
    print("\nMake sure:")
    print("- config.yaml exists in the same folder")
    print("- All module files are present")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nCheck:")
    print("- Your API keys are valid")
    print("- search_engine_id is correct in config.yaml")
    print("- All dependencies are installed: pip install requests pyyaml selenium pandas")
