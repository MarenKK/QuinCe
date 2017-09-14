package uk.ac.exeter.QuinCe.web.User;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;

import uk.ac.exeter.QuinCe.User.User;
import uk.ac.exeter.QuinCe.User.UserDB;
import uk.ac.exeter.QuinCe.User.UserPreferences;
import uk.ac.exeter.QuinCe.web.BaseManagedBean;
import uk.ac.exeter.QuinCe.web.system.ServletUtils;

/**
 * JSF Managed Bean for handling user logins
 * @author Steve Jones
 *
 */
@ManagedBean
@RequestScoped
public class LoginBean extends BaseManagedBean {
	
	public static final String SIGNUP_RESULT = "SignUp";
	
	public static final String AUTHENTICATION_FAILED_RESULT = "AuthenticationFailed";
	
	public static final String AUTHENTICATION_OK_RESULT = "AuthenticationSuccess";
	
	public static final String USER_SESSION_ATTR = "User";
	
	public static final String USER_PREFS_ATTR = "UserPrefs";
	
	private String emailAddress = null;
	
	private String password = null;
	
	public LoginBean() {
		// Do nothing
	}
	
	public String getEmailAddress() {
		return emailAddress;
	}

	public void setEmailAddress(String emailAddress) {
		this.emailAddress = emailAddress;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String authenticate() {
		
		String result = AUTHENTICATION_FAILED_RESULT;
		
		try {
			// Clear any existing user bean
			int authenticateResult = UserDB.authenticate(ServletUtils.getDBDataSource(), emailAddress, password.toCharArray());
			
			switch (authenticateResult) {
			case UserDB.AUTHENTICATE_OK: {
				User user = UserDB.getUser(getDataSource(), emailAddress);
				getSession().setAttribute(USER_SESSION_ATTR, user);
				UserPreferences prefs = UserDB.getPreferences(getDataSource(), user.getDatabaseID());
				getSession().setAttribute(USER_PREFS_ATTR, prefs);
				result = AUTHENTICATION_OK_RESULT;
				break;
			}
			case UserDB.AUTHENTICATE_FAILED: {
				setMessage(null, "The email address and/or password were not recognised");
				break;
			}
			case UserDB.AUTHENTICATE_EMAIL_CODE_SET: {
				setMessage(null, "Your account has not yet been activated. Please use the link in your activation email.");
				break;
			}
			}
			
		} catch (Exception e) {
			internalError(e);
		}
		
		getSession().removeAttribute("SESSION_EXPIRED");
		return result;
	}
	
	public String signUp() {
		return SIGNUP_RESULT;
	}

	@Override
	protected String getFormName() {
		return "loginform";
	}
}
