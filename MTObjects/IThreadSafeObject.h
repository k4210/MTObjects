#pragma once

#ifndef TEST_STUFF
//#define TEST_STUFF
#endif

#include <unordered_set>
#include <vector>
#include <algorithm>
#ifdef TEST_STUFF 
#include <assert.h>
#endif //TEST_STUFF
#include <deque>
#include <iterator>
namespace MTObjects
{
using std::unordered_set;
using std::vector;
using std::deque;

#ifdef TEST_STUFF 
struct TestStuff
{
	static unsigned int cluster_in_obj_overwritten;
	static unsigned int objects_to_handle;
};

#define Assert assert

#else

#define Assert(expression) ((void)0)

#endif //TEST_STUFF

template<typename T, unsigned int N> struct SmartArray
{
private:
	unsigned int size_ = 0;
	T data_[N];
public:
	T* begin() { return data_; }
	T* end() { return data_ + size_; }
	const T* begin() const { return data_; }
	const T* end() const { return data_ + size_; }

	unsigned int size() const { return size_; }
	bool empty() const { return 0 == size_; }

	void pop_back() { Assert(size_); size_--; }
	T& back() { Assert(size_); return data_[size_ - 1]; }
	const T& back() const { Assert(size_); return data_[size_ - 1]; }

	void push_back(const T& value) 
	{ 
		Assert(size_ < N);
		data_[size_] = value;
		size_++;
	}

	const T& operator[](unsigned int pos) const { return data[pos]; }
	T& operator[](unsigned int pos) { return data[pos]; }

	template<class Iter>
	void insert_back(Iter begin, Iter end)
	{
		Assert(std::distance(begin, end) < static_cast<int>(N - size_));
		while (begin != end)
		{
			data_[size_] = *begin;
			begin++;
			size_++;
		}
	}
};



struct IThreadSafeObject;
typedef SmartArray<IThreadSafeObject*, 128> ThreadSafeObjectsArray;

struct IThreadSafeObject
{
private:
	friend struct Cluster;
	Cluster* cluster_ = nullptr;

public:
	virtual void IsDependentOn(ThreadSafeObjectsArray& ref_dependencies) const = 0;
	virtual void IsConstDependentOn(vector<const IThreadSafeObject*>& ref_dependencies) const = 0;
};

struct Cluster
{
	vector<IThreadSafeObject*> objects_;			  //no duplicates
	vector<const IThreadSafeObject*> const_dependencies_; //with duplicates
	unordered_set<const Cluster*> const_dependencies_clusters_;

	int index = -1;

	void Reset()
	{
		objects_.clear();
		const_dependencies_.clear();
		const_dependencies_clusters_.clear();
		index = -1;
	}

	Cluster()
	{
		objects_.reserve(128);
		const_dependencies_.reserve(128);
		const_dependencies_clusters_.reserve(128);
		Reset();
	}

private:

	void MergeTo(Cluster& main_cluster)
	{
		for (auto obj : objects_)
		{
			obj->cluster_ = &main_cluster;
#ifdef TEST_STUFF 
			TestStuff::cluster_in_obj_overwritten++;
#endif //TEST_STUFF
		}
		main_cluster.objects_.insert(main_cluster.objects_.end(), objects_.begin(), objects_.end());
		main_cluster.const_dependencies_.insert(main_cluster.const_dependencies_.end(), const_dependencies_.begin(), const_dependencies_.end());

		//At this stage const_dependencies_clusters_ are not generated yet
	}

	static void RemoveCluster(vector<Cluster*> &clusters, const Cluster* cluster)
	{
		const int index_to_reuse = cluster->index;
		if (index_to_reuse != -1)
		{
			const int last_index = clusters.size() - 1;
			if (last_index != index_to_reuse)
			{
				Cluster* const moved_cluster = clusters[last_index];
				clusters[index_to_reuse] = moved_cluster;
				moved_cluster->index = index_to_reuse;
			}
			clusters.pop_back();
		}
	}

	static Cluster* GatherObjects(Cluster* new_cluster, IThreadSafeObject* in_obj, vector<Cluster*>& clusters)
	{
		Cluster* actual_cluster = new_cluster;
		ThreadSafeObjectsArray objects_to_handle;
		objects_to_handle.push_back(in_obj);
		while (!objects_to_handle.empty())
		{
			IThreadSafeObject* obj = objects_to_handle.back();
			objects_to_handle.pop_back();
			if (nullptr == obj->cluster_)
			{
				actual_cluster->objects_.emplace_back(obj);
				obj->cluster_ = actual_cluster;
				obj->IsConstDependentOn(actual_cluster->const_dependencies_);
				obj->IsDependentOn(objects_to_handle);
#ifdef TEST_STUFF 
				TestStuff::objects_to_handle = std::max(objects_to_handle.size(), TestStuff::objects_to_handle);
#endif //TEST_STUFF
				
			}
			else if (obj->cluster_ != actual_cluster)
			{
				const bool use_new_cluster = obj->cluster_->objects_.size() > actual_cluster->objects_.size();
				Cluster* to_merge = use_new_cluster ? actual_cluster : obj->cluster_;
				actual_cluster = use_new_cluster ? obj->cluster_ : actual_cluster;

				to_merge->MergeTo(*actual_cluster);
				RemoveCluster(clusters, to_merge);
				to_merge->Reset();
			}
		}

		return actual_cluster;
	}

	static void CreateClusters(const vector<IThreadSafeObject *> &all_objects, vector<Cluster*> &clusters, deque<Cluster>& preallocated_clusters)
	{
		unsigned int cluster_counter = 0;
		for(unsigned int first_remaining_obj_index = 0; first_remaining_obj_index < all_objects.size(); first_remaining_obj_index++)
		{
			if(nullptr != all_objects[first_remaining_obj_index]->cluster_)
			{
				continue;
			}

			Assert(cluster_counter < preallocated_clusters.size());
			Cluster* new_cluster = &preallocated_clusters[cluster_counter];
			Cluster* acltual_cluster = GatherObjects(new_cluster, all_objects[first_remaining_obj_index], clusters);
			if (acltual_cluster == new_cluster)
			{
				new_cluster->index = clusters.size();
				clusters.emplace_back(new_cluster);
				cluster_counter++;
			}
		}
	}

	static void CreateClustersDependencies(const vector<Cluster*>& clusters)
	{
		for (auto cluster : clusters)
		{
			//Generate cluster dependencies. It can be done, only when all objects have cluster set.
			std::transform(cluster->const_dependencies_.begin(), cluster->const_dependencies_.end()
				, std::inserter(cluster->const_dependencies_clusters_, cluster->const_dependencies_clusters_.begin())
				, [](const IThreadSafeObject* obj) { return obj->cluster_; });
			cluster->const_dependencies_clusters_.erase(cluster);
		}
	}

public:

	static void ClearClustersInObjects(const vector<IThreadSafeObject*>& all_objects)
	{
		for (auto obj : all_objects)
		{
			obj->cluster_ = nullptr;
		}
	}

	static vector<Cluster*> GenerateClusters(const vector<IThreadSafeObject*>& all_objects, deque<Cluster>& preallocated_clusters)
	{
		vector<Cluster*> clusters;
		clusters.reserve(preallocated_clusters.size());
		CreateClusters(all_objects, clusters, preallocated_clusters);
		CreateClustersDependencies(clusters);
		return clusters;
	}
#ifdef TEST_STUFF 
	static bool Test_AreClustersCoherent(const vector<Cluster*>& clusters)
	{
		// All dependencies of the objects must be inside the cluster
		for (auto cluster : clusters)
		{
			auto& objects = cluster->objects_;
			for (auto obj : objects)
			{
				Assert(obj && obj->cluster_ == cluster);

				ThreadSafeObjectsArray dependencies;
				obj->IsDependentOn(dependencies);
				for (auto dep : dependencies)
				{
					Assert(dep && dep->cluster_ == cluster);
				}
			}
			std::sort(objects.begin(), objects.end());
			const int last_index = objects.size() - 1;
			for (int i = 1; i < last_index; i++)
			{
				Assert(objects[i] != objects[i+1]);
			}
		}
		return true;
	}
#endif //TEST_STUFF
};

struct GroupOfConcurrentClusters : public vector<Cluster*>
{
	unordered_set<const Cluster*> const_dependencies_clusters_;

	void AddCluster(Cluster& cluster)
	{
		emplace_back(&cluster);
		const_dependencies_clusters_.insert(cluster.const_dependencies_clusters_.begin(), cluster.const_dependencies_clusters_.end());
	}

	static vector<GroupOfConcurrentClusters> GenerateClusterGroups(const vector<Cluster*>& clusters)
	{
		auto depends_on = [](const Cluster* a, const Cluster* b) -> bool
		{
			return a->const_dependencies_clusters_.end() != a->const_dependencies_clusters_.find(b);
		};

		auto fits_into_group = [&](const GroupOfConcurrentClusters& group, const Cluster* cluster) -> bool
		{
			return std::none_of(group.begin(), group.end(), [&](const Cluster* cluster_in_group)
			{
				return depends_on(cluster, cluster_in_group);
			}) 

			&& group.const_dependencies_clusters_.find(cluster) == group.const_dependencies_clusters_.end();
		};

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size());
		groups.resize(std::max<int>(1, clusters.size() / 32));
		int cluster_index = 0;
		for (auto cluster : clusters)
		{
			bool fits_in_existing_group = false;
			{
				const int group_num = groups.size();
				const int first_group_to_try = cluster_index % group_num;
				for (int groups_checked = 0; groups_checked < group_num && !fits_in_existing_group; groups_checked++)
				{
					const int group_idx = (first_group_to_try + groups_checked) % group_num;
					auto& group = groups[group_idx];
					if (fits_into_group(group, cluster))
					{
						group.AddCluster(*cluster);
						fits_in_existing_group = true;
					}
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.AddCluster(*cluster);
				groups.emplace_back(new_group);
			}
			cluster_index++;
		}
		return groups;
	}
};
}
