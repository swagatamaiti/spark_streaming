package com.mutliOrder.DataBean.bean;

import java.util.ArrayList;
import java.util.List;

import com.mutliOrder.DataBean.bean.base.Model_Base;
import com.mutliOrder.common.String.ListUtils;
import com.mutliOrder.common.String.StringUtils;

/**
 * app详细信息
 * 
 * @author Administrator
 *
 */
public class Model_AppInfo extends Model_Base {
	final static String Hbase_Table = "App_Base";
	final static String[] Unique_Key_Fields= new String[]{"AppId","ReleaseDate"};
	private Integer AppId;
	private String NameRaw;
	private String Name;
	private String ArtistId;
	private String ArtistName;
	private String ArtistUrl;
	private String RequiredCapabilities;
	private String Kind;
	private String Copyright;
	private String ReleaseDate;
	private Object VersionHistory;
	private String ArtworkUrl;
	private Object EditorialArtworkUrl;
	private String Description;
	private String FileSizeByDevice;
	private Object Genres;
	private String Url;
	private String ShortUrl;
	private Object UserRating;
	private Object VideoPreviewByType;
	private String FamilyShareEnabledDate;
	private String messagesScreenshots;
	private Object RatingAndAdvisories;
	private Object Offers;
	private Object ScreenshotsByType;
	private Object SoftwareInfo;
	private boolean Disable;
	private String BundleId;
	private List<String> CustomersAlsoBoughtApps;

	public String getBundleId() {
		return BundleId;
	}

	public void setBundleId(String bundleId) {
		BundleId = bundleId;
	}

	public List<String> getCustomersAlsoBoughtApps() {
		return CustomersAlsoBoughtApps;
	}

	public void setCustomersAlsoBoughtApps(List<String> customersAlsoBoughtApps) {
		CustomersAlsoBoughtApps = customersAlsoBoughtApps;
	}

	public Object getSoftwareInfo() {
		return SoftwareInfo;
	}

	public void setSoftwareInfo(Object softwareInfo) {
		SoftwareInfo = softwareInfo;
	}

	public Object getScreenshotsByType() {
		return ScreenshotsByType;
	}

	public void setScreenshotsByType(Object screenshotsByType) {
		ScreenshotsByType = screenshotsByType;
	}

	public boolean getDisable() {
		return Disable;
	}

	public void setDisable(boolean disable) {
		Disable = disable;
	}

	public Integer getAppId() {
		return AppId;
	}

	public void setAppId(Integer appId) {
		AppId = appId;
	}

	public String getNameRaw() {
		return NameRaw;
	}

	public void setNameRaw(String nameRaw) {
		NameRaw = nameRaw;
	}

	public String getName() {
		return Name;
	}

	public void setName(String name) {
		Name = name;
	}

	public String getArtistId() {
		return ArtistId;
	}

	public void setArtistId(String artistId) {
		ArtistId = artistId;
	}

	public String getArtistName() {
		return ArtistName;
	}

	public void setArtistName(String artistName) {
		ArtistName = artistName;
	}

	public String getArtistUrl() {
		return ArtistUrl;
	}

	public void setArtistUrl(String artistUrl) {
		ArtistUrl = artistUrl;
	}

	public String getRequiredCapabilities() {
		return RequiredCapabilities;
	}

	public void setRequiredCapabilities(String requiredCapabilities) {
		RequiredCapabilities = requiredCapabilities;
	}

	public String getKind() {
		return Kind;
	}

	public void setKind(String kind) {
		Kind = kind;
	}

	public String getCopyright() {
		return Copyright;
	}

	public void setCopyright(String copyright) {
		Copyright = copyright;
	}

	public String getReleaseDate() {
		return ReleaseDate;
	}

	public void setReleaseDate(String releaseDate) {
		ReleaseDate = releaseDate;
	}

	public Object getVersionHistory() {
		return VersionHistory;
	}

	public void setVersionHistory(Object versionHistory) {
		VersionHistory = versionHistory;
	}

	public String getArtworkUrl() {
		return ArtworkUrl;
	}

	public void setArtworkUrl(String artworkUrl) {
		ArtworkUrl = artworkUrl;
	}

	public Object getEditorialArtworkUrl() {
		return EditorialArtworkUrl;
	}

	public void setEditorialArtworkUrl(Object editorialArtworkUrl) {
		EditorialArtworkUrl = editorialArtworkUrl;
	}

	public String getDescription() {
		return Description;
	}

	public void setDescription(String description) {
		Description = description;
	}

	public String getFileSizeByDevice() {
		return FileSizeByDevice;
	}

	public void setFileSizeByDevice(String fileSizeByDevice) {
		FileSizeByDevice = fileSizeByDevice;
	}

	public Object getGenres() {
		return Genres;
	}

	public void setGenres(Object genres) {
		Genres = genres;
	}

	public String getUrl() {
		return Url;
	}

	public void setUrl(String url) {
		Url = url;
	}

	public String getShortUrl() {
		return ShortUrl;
	}

	public void setShortUrl(String shortUrl) {
		ShortUrl = shortUrl;
	}

	public Object getUserRating() {
		return UserRating;
	}

	public void setUserRating(Object userRating) {
		UserRating = userRating;
	}

	public Object getVideoPreviewByType() {
		return VideoPreviewByType;
	}

	public void setVideoPreviewByType(Object videoPreviewByType) {
		VideoPreviewByType = videoPreviewByType;
	}

	public String getFamilyShareEnabledDate() {
		return FamilyShareEnabledDate;
	}

	public void setFamilyShareEnabledDate(String familyShareEnabledDate) {
		FamilyShareEnabledDate = familyShareEnabledDate;
	}

	public String getMessagesScreenshots() {
		return messagesScreenshots;
	}

	public void setMessagesScreenshots(String messagesScreenshots) {
		this.messagesScreenshots = messagesScreenshots;
	}

	public Object getRatingAndAdvisories() {
		return RatingAndAdvisories;
	}

	public void setRatingAndAdvisories(Object ratingAndAdvisories) {
		RatingAndAdvisories = ratingAndAdvisories;
	}

	public Object getOffers() {
		return Offers;
	}

	public void setOffers(Object offers) {
		Offers = offers;
	}

	public Model_AppInfo() {
		
		super();
		this.hbaseTable = Hbase_Table;
		this.solrCollection=Hbase_Table;
		this.uniqueKeyField = Unique_Key_Fields;
	}

	public Model_AppInfo(int appId) {
		super();
		this.hbaseTable = Hbase_Table;
		this.uniqueKeyField = Unique_Key_Fields;
		this.solrCollection=Hbase_Table;
		this.AppId = appId;
	}
	
	@Override
	public void clean() {
		// TODO Auto-generated method stub
		super.clean();
		if("0001-01-01T00:00:00".equals(this.ReleaseDate)||"0001-01-01 00:00:00".equals(this.ReleaseDate)){
			this.ReleaseDate = null;
		}
	}
	
	@Override
	public Model_Base merge(Model_Base mb) {
		if(((Model_AppInfo)mb).getDisable()&&!this.Disable){
			if (mb.getFetchTime() != null && this.getFetchTime() != null) {
				if (this.getFetchTime().compareTo(mb.getFetchTime()) <= 0) {
					((Model_AppInfo)this).setDisable(true);
				} 
			}
			Model_Base mm=this;
			try {
				mm = this.myClone();
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return mm;
		}else if(((Model_AppInfo)mb).getDisable()&&this.Disable){
			return this;
		}else {
			Model_AppInfo ma = (Model_AppInfo)mb;
			Model_AppInfo thisMa = (Model_AppInfo)this;
			if(StringUtils.filedIsNull(thisMa.getBundleId())&&!StringUtils.filedIsNull(ma.getBundleId())) {
				return ma;
			}
			if(ListUtils.isEmpty(thisMa.getCustomersAlsoBoughtApps())&&!ListUtils.isEmpty(ma.getCustomersAlsoBoughtApps())) {
				return ma;
			}else if((!ListUtils.isEmpty(thisMa.getCustomersAlsoBoughtApps()))&&(!ListUtils.isEmpty(ma.getCustomersAlsoBoughtApps()))){
				if(thisMa.getCustomersAlsoBoughtApps().size()!=ma.getCustomersAlsoBoughtApps().size()) {
					return ma;
				}else {
					List<String> tempList = new ArrayList<String>();
					tempList.addAll(thisMa.getCustomersAlsoBoughtApps());
					tempList.removeAll(ma.getCustomersAlsoBoughtApps());
					if(tempList.isEmpty()) {
						return this;
					}else {
						return ma;
					}
				}				
			}
			return this;
		}
	}
}
