package com.folder.utility;

/**
 * @author Bitwise Offshore
 * 
 */
public enum LiteralConstant {
	CURRENT_IND{
		@Override
		public 	String toString(){
			return "current_ind";
		}
	},
	DW_UPDATE_DTM{
		@Override
		public 	String toString(){
			return "dw_update_date_time";
		}
	},
	YYYY_MM_DD_HH_MM_SS{
		@Override
		public String toString(){
			return "yyyy-MM-dd HH:mm:ss";
		}
	},
	YYYY_MM_DD{
		@Override
		public String toString(){
			return "yyyy-MM-dd";
		}
	},
	
	MASTER{
		@Override
		public String toString(){
			return "master";
		}
	},
	CHILD{
		@Override
		public String toString(){
			return "child";
		}
	},
	
	MERCHANT{
		@Override
		public String toString(){
			return "merchant_number";
		}
	},
	
	SOFT_DELETE_KEY_DELIMITER{
		@Override
		public String toString(){
			return "\\|";
		}
	},
	SOFT_DELETE_KEY_DELIMITER1{
		@Override
		public String toString(){
			return ",";
		}
	};
		
}
